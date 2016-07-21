package mdata

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/dgryski/go-tsz"
	"github.com/gocql/gocql"
	"github.com/raintank/met"
	"github.com/raintank/metrictank/iter"
	"github.com/raintank/metrictank/mdata/chunk"
	"github.com/raintank/worldping-api/pkg/log"
)

// write aggregated data to cassandra.

const Month_sec = 60 * 60 * 24 * 28

const keyspace_schema = `CREATE KEYSPACE IF NOT EXISTS raintank WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}  AND durable_writes = true`
const table_schema = `CREATE TABLE IF NOT EXISTS raintank.metric (
    key ascii,
    ts int,
    data blob,
    PRIMARY KEY (key, ts)
) WITH COMPACT STORAGE
    AND CLUSTERING ORDER BY (ts DESC)
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.DateTieredCompactionStrategy'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND read_repair_chance = 0.0
    AND dclocal_read_repair_chance = 0`

var (
	errChunkTooSmall      = errors.New("unpossibly small chunk in cassandra")
	errUnknownChunkFormat = errors.New("unrecognized chunk format in cassandra")
	errStartBeforeEnd     = errors.New("start must be before end.")

	cassBlockDuration met.Timer
	cassGetDuration   met.Timer
	cassPutDuration   met.Timer

	cassChunksPerRow    met.Meter
	cassRowsPerResponse met.Meter
	cassToIterDuration  met.Timer

	chunkSaveOk   met.Count
	chunkSaveFail met.Count
	// it's pretty expensive/impossible to do chunk size in mem vs in cassandra etc, but we can more easily measure chunk sizes when we operate on them
	chunkSizeAtSave met.Meter
	chunkSizeAtLoad met.Meter
)

/*
https://godoc.org/github.com/gocql/gocql#Session
Session is the interface used by users to interact with the database.
It's safe for concurrent use by multiple goroutines and a typical usage scenario is to have one global session
object to interact with the whole Cassandra cluster.
*/

type cassandraStore struct {
	session          *gocql.Session
	writeQueues      []chan *ChunkWriteRequest
	readQueue        chan *ChunkReadRequest
	writeQueueMeters []met.Meter
}

func NewCassandraStore(stats met.Backend, addrs, consistency string, timeout, readers, writers, readqsize, writeqsize int) (*cassandraStore, error) {
	cluster := gocql.NewCluster(strings.Split(addrs, ",")...)
	cluster.Consistency = gocql.ParseConsistency(consistency)
	cluster.Timeout = time.Duration(timeout) * time.Millisecond
	cluster.NumConns = writers
	var err error
	tmpSession, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}
	// ensure the keyspace and table exist.
	err = tmpSession.Query(keyspace_schema).Exec()
	if err != nil {
		return nil, err
	}
	err = tmpSession.Query(table_schema).Exec()
	if err != nil {
		return nil, err
	}
	tmpSession.Close()
	cluster.Keyspace = "raintank"
	session, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}
	c := &cassandraStore{
		session:          session,
		writeQueues:      make([]chan *ChunkWriteRequest, writers),
		readQueue:        make(chan *ChunkReadRequest, readqsize),
		writeQueueMeters: make([]met.Meter, writers),
	}

	for i := 0; i < writers; i++ {
		c.writeQueues[i] = make(chan *ChunkWriteRequest, writeqsize)
		c.writeQueueMeters[i] = stats.NewMeter(fmt.Sprintf("cassandra.write_queue.%d.items", i+1), 0)
		go c.processWriteQueue(c.writeQueues[i], c.writeQueueMeters[i])
	}

	for i := 0; i < readers; i++ {
		go c.processReadQueue()
	}

	return c, err
}

func (c *cassandraStore) InitMetrics(stats met.Backend) {
	cassBlockDuration = stats.NewTimer("cassandra.block_duration", 0)
	cassGetDuration = stats.NewTimer("cassandra.get_duration", 0)
	cassPutDuration = stats.NewTimer("cassandra.put_duration", 0)

	cassChunksPerRow = stats.NewMeter("cassandra.chunks_per_row", 0)
	cassRowsPerResponse = stats.NewMeter("cassandra.rows_per_response", 0)
	cassToIterDuration = stats.NewTimer("cassandra.to_iter_duration", 0)

	chunkSaveOk = stats.NewCount("chunks.save_ok")
	chunkSaveFail = stats.NewCount("chunks.save_fail")
	chunkSizeAtSave = stats.NewMeter("chunk_size.at_save", 0)
	chunkSizeAtLoad = stats.NewMeter("chunk_size.at_load", 0)
}

func (c *cassandraStore) Add(cwr *ChunkWriteRequest) {
	sum := 0
	for _, char := range cwr.key {
		sum += int(char)
	}
	which := sum % len(c.writeQueues)
	c.writeQueueMeters[which].Value(int64(len(c.writeQueues[which])))
	c.writeQueues[which] <- cwr
	c.writeQueueMeters[which].Value(int64(len(c.writeQueues[which])))
}

/* process writeQueue.
 */
func (c *cassandraStore) processWriteQueue(queue chan *ChunkWriteRequest, meter met.Meter) {
	tick := time.Tick(time.Duration(1) * time.Second)
	for {
		select {
		case <-tick:
			meter.Value(int64(len(queue)))
		case cwr := <-queue:
			meter.Value(int64(len(queue)))
			log.Debug("CS: starting to save %s:%d %v", cwr.key, cwr.chunk.T0, cwr.chunk)
			//log how long the chunk waited in the queue before we attempted to save to cassandra
			cassBlockDuration.Value(time.Now().Sub(cwr.timestamp))

			data := cwr.chunk.Series.Bytes()
			chunkSizeAtSave.Value(int64(len(data)))
			version := chunk.FormatStandardGoTsz
			buf := new(bytes.Buffer)
			binary.Write(buf, binary.LittleEndian, uint8(version))
			buf.Write(data)
			success := false
			attempts := 0
			for !success {
				err := c.insertChunk(cwr.key, cwr.chunk.T0, buf.Bytes(), int(cwr.ttl))
				if err == nil {
					success = true
					cwr.chunk.Saved = true
					SendPersistMessage(cwr.key, cwr.chunk.T0)
					log.Debug("CS: save complete. %s:%d %v", cwr.key, cwr.chunk.T0, cwr.chunk)
					chunkSaveOk.Inc(1)
				} else {
					if (attempts % 20) == 0 {
						log.Warn("CS: failed to save chunk to cassandra after %d attempts. %v, %s", attempts+1, cwr.chunk, err)
					}
					chunkSaveFail.Inc(1)
					sleepTime := 100 * attempts
					if sleepTime > 2000 {
						sleepTime = 2000
					}
					time.Sleep(time.Duration(sleepTime) * time.Millisecond)
					attempts++
				}
			}
		}
	}
}

// Insert Chunks into Cassandra.
//
// key: is the metric_id
// ts: is the start of the aggregated time range.
// data: is the payload as bytes.
func (c *cassandraStore) insertChunk(key string, t0 uint32, data []byte, ttl int) error {
	// for unit tests
	if c.session == nil {
		return nil
	}
	query := fmt.Sprintf("INSERT INTO metric (key, ts, data) values(?,?,?) USING TTL %d", ttl)
	row_key := fmt.Sprintf("%s_%d", key, t0/Month_sec) // "month number" based on unix timestamp (rounded down)
	pre := time.Now()
	ret := c.session.Query(query, row_key, t0, data).Exec()
	cassPutDuration.Value(time.Now().Sub(pre))
	return ret
}

type outcome struct {
	month   uint32
	sortKey uint32
	i       *gocql.Iter
}
type asc []outcome

func (o asc) Len() int           { return len(o) }
func (o asc) Swap(i, j int)      { o[i], o[j] = o[j], o[i] }
func (o asc) Less(i, j int) bool { return o[i].sortKey < o[j].sortKey }

func (c *cassandraStore) processReadQueue() {
	for crr := range c.readQueue {
		crr.out <- outcome{crr.month, crr.sortKey, c.session.Query(crr.q, crr.p...).Iter()}
	}
}

// Basic search of cassandra.
// start inclusive, end exclusive
func (c *cassandraStore) Search(key string, start, end uint32) ([]iter.Iter, error) {
	iters := make([]iter.Iter, 0)
	if start > end {
		return iters, errStartBeforeEnd
	}

	crrs := make([]*ChunkReadRequest, 0)

	query := func(month, sortKey uint32, q string, p ...interface{}) {
		crrs = append(crrs, &ChunkReadRequest{month, sortKey, q, p, nil})
	}

	start_month := start - (start % Month_sec)       // starting row has to be at, or before, requested start
	end_month := (end - 1) - ((end - 1) % Month_sec) // ending row has to be include the last point we might need

	pre := time.Now()

	// unfortunately in the database we only have the t0's of all chunks.
	// this means we can easily make sure to include the correct last chunk (just query for a t0 < end, the last chunk will contain the last needed data)
	// but it becomes hard to find which should be the first chunk to include. we can't just query for start <= t0 because than will miss some data at the beginning
	// we can't assume we know the chunkSpan so we can't just calculate the t0 >= start - <some-predefined-number> because chunkSpans may change over time.
	// we effectively need all chunks with a t0 > start, as well as the last chunk with a t0 <= start.
	// since we make sure that you can only use chunkSpans so that Month_sec % chunkSpan == 0, we know that this previous chunk will always be in the same row
	// as the one that has start_month.

	row_key := fmt.Sprintf("%s_%d", key, start_month/Month_sec)

	query(start_month, start_month, "SELECT ts, data FROM metric WHERE key=? AND ts <= ? Limit 1", row_key, start)

	if start_month == end_month {
		// we need a selection of the row between startTs and endTs
		row_key = fmt.Sprintf("%s_%d", key, start_month/Month_sec)
		query(start_month, start_month+1, "SELECT ts, data FROM metric WHERE key = ? AND ts > ? AND ts < ? ORDER BY ts ASC", row_key, start, end)
	} else {
		// get row_keys for each row we need to query.
		for month := start_month; month <= end_month; month += Month_sec {
			row_key = fmt.Sprintf("%s_%d", key, month/Month_sec)
			if month == start_month {
				// we want from startTs to the end of the row.
				query(month, month+1, "SELECT ts, data FROM metric WHERE key = ? AND ts >= ? ORDER BY ts ASC", row_key, start+1)
			} else if month == end_month {
				// we want from start of the row till the endTs.
				query(month, month, "SELECT ts, data FROM metric WHERE key = ? AND ts <= ? ORDER BY ts ASC", row_key, end-1)
			} else {
				// we want all columns
				query(month, month, "SELECT ts, data FROM metric WHERE key = ? ORDER BY ts ASC", row_key)
			}
		}
	}
	numQueries := len(crrs)
	results := make(chan outcome, numQueries)
	for i := range crrs {
		crrs[i].out = results
		c.readQueue <- crrs[i]
	}
	outcomes := make([]outcome, 0, numQueries)

	seen := 0
	for o := range results {
		seen += 1
		outcomes = append(outcomes, o)
		if seen == numQueries {
			close(results)
			cassGetDuration.Value(time.Now().Sub(pre))
			break
		}
	}
	pre = time.Now()
	// we have all of the results, but they could have arrived in any order.
	sort.Sort(asc(outcomes))

	var b []byte
	var ts int
	for _, outcome := range outcomes {
		chunks := int64(0)
		for outcome.i.Scan(&ts, &b) {
			chunks += 1
			chunkSizeAtLoad.Value(int64(len(b)))
			if len(b) < 2 {
				log.Error(3, errChunkTooSmall.Error())
				return iters, errChunkTooSmall
			}
			if chunk.Format(b[0]) != chunk.FormatStandardGoTsz {
				log.Error(3, errUnknownChunkFormat.Error())
				return iters, errUnknownChunkFormat
			}
			it, err := tsz.NewIterator(b[1:])
			if err != nil {
				log.Error(3, "failed to unpack cassandra payload. %s", err)
				return iters, err
			}
			iters = append(iters, iter.New(it, true))
		}
		cassChunksPerRow.Value(chunks)
		err := outcome.i.Close()
		if err != nil {
			log.Error(3, "cassandra query error. %s", err)
		}
	}
	cassToIterDuration.Value(time.Now().Sub(pre))
	cassRowsPerResponse.Value(int64(len(outcomes)))
	log.Debug("CS: searchCassandra(): %d outcomes (queries), %d total iters", len(outcomes), len(iters))
	return iters, nil
}

func (c *cassandraStore) Stop() {
	c.session.Close()
}
