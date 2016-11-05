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
	"github.com/hailocab/go-hostpool"
	"github.com/raintank/met"
	"github.com/raintank/metrictank/cassandra"
	"github.com/raintank/metrictank/iter"
	"github.com/raintank/metrictank/mdata/chunk"
	"github.com/raintank/worldping-api/pkg/log"
)

// write aggregated data to cassandra.

const Month_sec = 60 * 60 * 24 * 28

const keyspace_schema = `CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}  AND durable_writes = true`
const table_schema = `CREATE TABLE IF NOT EXISTS %s.metric (
    key ascii,
    ts int,
    data blob,
    PRIMARY KEY (key, ts)
) WITH CLUSTERING ORDER BY (ts DESC)
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy', 'compaction_window_unit': 'DAYS', 'compaction_window_size': '1' }
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'}`

var (
	errChunkTooSmall      = errors.New("unpossibly small chunk in cassandra")
	errUnknownChunkFormat = errors.New("unrecognized chunk format in cassandra")
	errStartBeforeEnd     = errors.New("start must be before end.")

	cassGetExecDuration met.Timer
	cassGetWaitDuration met.Timer
	cassPutExecDuration met.Timer
	cassPutWaitDuration met.Timer

	cassChunksPerRow      met.Meter
	cassRowsPerResponse   met.Meter
	cassGetChunksDuration met.Timer
	cassToIterDuration    met.Timer

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
	metrics          cassandra.Metrics
}

func NewCassandraStore(stats met.Backend, addrs, keyspace, consistency, CaPath, Username, Password, hostSelectionPolicy string, timeout, readers, writers, readqsize, writeqsize, retries, protoVer int, ssl, auth, hostVerification bool) (*cassandraStore, error) {
	cluster := gocql.NewCluster(strings.Split(addrs, ",")...)
	if ssl {
		cluster.SslOpts = &gocql.SslOptions{
			CaPath:                 CaPath,
			EnableHostVerification: hostVerification,
		}
	}
	if auth {
		cluster.Authenticator = gocql.PasswordAuthenticator{
			Username: Username,
			Password: Password,
		}
	}
	cluster.Consistency = gocql.ParseConsistency(consistency)
	cluster.Timeout = time.Duration(timeout) * time.Millisecond
	cluster.NumConns = writers
	cluster.ProtoVersion = protoVer
	var err error
	tmpSession, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}
	// ensure the keyspace and table exist.
	err = tmpSession.Query(fmt.Sprintf(keyspace_schema, keyspace)).Exec()
	if err != nil {
		return nil, err
	}
	err = tmpSession.Query(fmt.Sprintf(table_schema, keyspace)).Exec()
	if err != nil {
		return nil, err
	}
	tmpSession.Close()
	cluster.Keyspace = keyspace
	cluster.RetryPolicy = &gocql.SimpleRetryPolicy{NumRetries: retries}

	switch hostSelectionPolicy {
	case "roundrobin":
		cluster.PoolConfig.HostSelectionPolicy = gocql.RoundRobinHostPolicy()
	case "hostpool-simple":
		cluster.PoolConfig.HostSelectionPolicy = gocql.HostPoolHostPolicy(hostpool.New(nil))
	case "hostpool-epsilon-greedy":
		cluster.PoolConfig.HostSelectionPolicy = gocql.HostPoolHostPolicy(
			hostpool.NewEpsilonGreedy(nil, 0, &hostpool.LinearEpsilonValueCalculator{}),
		)
	case "tokenaware,roundrobin":
		cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(
			gocql.RoundRobinHostPolicy(),
		)
	case "tokenaware,hostpool-simple":
		cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(
			gocql.HostPoolHostPolicy(hostpool.New(nil)),
		)
	case "tokenaware,hostpool-epsilon-greedy":
		cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(
			gocql.HostPoolHostPolicy(
				hostpool.NewEpsilonGreedy(nil, 0, &hostpool.LinearEpsilonValueCalculator{}),
			),
		)
	default:
		return nil, fmt.Errorf("unknown HostSelectionPolicy '%q'", hostSelectionPolicy)
	}

	session, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}
	log.Debug("CS: created session to %s keysp %s cons %v with policy %s timeout %d readers %d writers %d readq %d writeq %d retries %d proto %d ssl %t auth %t hostverif %t", addrs, keyspace, consistency, hostSelectionPolicy, timeout, readers, writers, readqsize, writeqsize, retries, protoVer, ssl, auth, hostVerification)
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
	cassGetExecDuration = stats.NewTimer("cassandra.get.exec", 0)
	cassGetWaitDuration = stats.NewTimer("cassandra.get.wait", 0)
	cassPutExecDuration = stats.NewTimer("cassandra.put.exec", 0)
	cassPutWaitDuration = stats.NewTimer("cassandra.put.wait", 0)

	cassChunksPerRow = stats.NewMeter("cassandra.chunks_per_row", 0)
	cassRowsPerResponse = stats.NewMeter("cassandra.rows_per_response", 0)
	cassGetChunksDuration = stats.NewTimer("cassandra.get_chunks", 0)
	cassToIterDuration = stats.NewTimer("cassandra.to_iter", 0)

	chunkSaveOk = stats.NewCount("chunks.save_ok")
	chunkSaveFail = stats.NewCount("chunks.save_fail")
	chunkSizeAtSave = stats.NewMeter("chunk_size.at_save", 0)
	chunkSizeAtLoad = stats.NewMeter("chunk_size.at_load", 0)

	c.metrics = cassandra.NewMetrics("cassandra", stats)
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
			cassPutWaitDuration.Value(time.Now().Sub(cwr.timestamp))

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
					c.metrics.Inc(err)
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
	cassPutExecDuration.Value(time.Now().Sub(pre))
	return ret
}

type outcome struct {
	sortKey int
	i       *gocql.Iter
}
type asc []outcome

func (o asc) Len() int           { return len(o) }
func (o asc) Swap(i, j int)      { o[i], o[j] = o[j], o[i] }
func (o asc) Less(i, j int) bool { return o[i].sortKey < o[j].sortKey }

func (c *cassandraStore) processReadQueue() {
	for crr := range c.readQueue {
		cassGetWaitDuration.Value(time.Since(crr.timestamp))
		pre := time.Now()
		iter := outcome{crr.sortKey, c.session.Query(crr.q, crr.p...).Iter()}
		cassGetExecDuration.Value(time.Since(pre))
		crr.out <- iter
	}
}

// Basic search of cassandra.
// start inclusive, end exclusive
func (c *cassandraStore) Search(key string, start, end uint32) ([]iter.Iter, error) {
	iters := make([]iter.Iter, 0)
	if start > end {
		return iters, errStartBeforeEnd
	}

	pre := time.Now()

	crrs := make([]*ChunkReadRequest, 0)

	query := func(sortKey int, q string, p ...interface{}) {
		crrs = append(crrs, &ChunkReadRequest{sortKey, q, p, time.Now(), nil})
	}

	start_month := start - (start % Month_sec)       // starting row has to be at, or before, requested start
	end_month := (end - 1) - ((end - 1) % Month_sec) // ending row has to include the last point we might need (end-1)

	// unfortunately in the database we only have the t0's of all chunks.
	// this means we can easily make sure to include the correct last chunk (just query for a t0 < end, the last chunk will contain the last needed data)
	// but it becomes hard to find which should be the first chunk to include. we can't just query for start <= t0 because than will miss some data at the beginning
	// we can't assume we know the chunkSpan so we can't just calculate the t0 >= start - <some-predefined-number> because chunkSpans may change over time.
	// we effectively need all chunks with a t0 > start, as well as the last chunk with a t0 <= start.
	// since we make sure that you can only use chunkSpans so that Month_sec % chunkSpan == 0, we know that this previous chunk will always be in the same row
	// as the one that has start_month.

	row_key := fmt.Sprintf("%s_%d", key, start_month/Month_sec)

	query(0, "SELECT ts, data FROM metric WHERE key=? AND ts <= ? Limit 1", row_key, start)

	if start_month == end_month {
		// we need a selection of the row between startTs and endTs
		row_key = fmt.Sprintf("%s_%d", key, start_month/Month_sec)
		query(1, "SELECT ts, data FROM metric WHERE key = ? AND ts > ? AND ts < ? ORDER BY ts ASC", row_key, start, end)
	} else {
		// get row_keys for each row we need to query.
		sortKey := 1
		for month := start_month; month <= end_month; month += Month_sec {
			row_key = fmt.Sprintf("%s_%d", key, month/Month_sec)
			if month == start_month {
				// we want from startTs to the end of the row.
				query(sortKey, "SELECT ts, data FROM metric WHERE key = ? AND ts >= ? ORDER BY ts ASC", row_key, start+1)
			} else if month == end_month {
				// we want from start of the row till the endTs.
				query(sortKey, "SELECT ts, data FROM metric WHERE key = ? AND ts <= ? ORDER BY ts ASC", row_key, end-1)
			} else {
				// we want all columns
				query(sortKey, "SELECT ts, data FROM metric WHERE key = ? ORDER BY ts ASC", row_key)
			}
			sortKey++
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
			break
		}
	}
	cassGetChunksDuration.Value(time.Since(pre))
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
		err := outcome.i.Close()
		if err != nil {
			log.Error(3, "cassandra query error. %s", err)
			c.metrics.Inc(err)
		} else {
			cassChunksPerRow.Value(chunks)
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
