package main

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/gocql/gocql"
	"github.com/grafana/grafana/pkg/log"
	//"github.com/dgryski/go-tsz"
	"github.com/raintank/go-tsz"
)

// write aggregated data to cassandra.

const month_sec = 60 * 60 * 24 * 28

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

/*
https://godoc.org/github.com/gocql/gocql#Session
Session is the interface used by users to interact with the database.
It's safe for concurrent use by multiple goroutines and a typical usage scenario is to have one global session
object to interact with the whole Cassandra cluster.
*/
var cSession *gocql.Session
var writeSem chan bool

func InitCassandra() error {
	writeSem = make(chan bool, *cassandraWriteConcurrency)
	cluster := gocql.NewCluster(strings.Split(*cassandraAddrs, ",")...)
	cluster.Consistency = gocql.One
	var err error
	tmpSession, err := cluster.CreateSession()
	if err != nil {
		return err
	}
	// ensure the keyspace and table exist.
	err = tmpSession.Query(keyspace_schema).Exec()
	if err != nil {
		return err
	}
	err = tmpSession.Query(table_schema).Exec()
	if err != nil {
		return err
	}
	tmpSession.Close()
	cluster.Keyspace = "raintank"
	cluster.NumConns = *cassandraWriteConcurrency
	cSession, err = cluster.CreateSession()
	return err
}

// Insert Chunks into Cassandra.
//
// key: is the metric_id
// ts: is the start of the aggregated time range.
// data: is the payload as bytes.
func InsertChunk(key string, t0 uint32, data []byte, ttl int) error {
	// increment our semaphore.
	// blocks if <cassandraWriteConcurrency> writers are already running
	pre := time.Now()
	writeSem <- true
	defer func() {
		// write is complete, so decrement our semaphore.
		<-writeSem
	}()
	cassandraBlockDuration.Value(time.Now().Sub(pre))
	// for unit tests
	if cSession == nil {
		return nil
	}
	query := fmt.Sprintf("INSERT INTO metric (key, ts, data) values(?,?,?) USING TTL %d", ttl)
	row_key := fmt.Sprintf("%s_%d", key, t0/month_sec) // "month number" based on unix timestamp (rounded down)
	pre = time.Now()
	ret := cSession.Query(query, row_key, t0, data).Exec()
	cassandraPutDuration.Value(time.Now().Sub(pre))
	return ret
}

type outcome struct {
	month uint32
	i     *gocql.Iter
}
type asc []outcome

func (o asc) Len() int           { return len(o) }
func (o asc) Swap(i, j int)      { o[i], o[j] = o[j], o[i] }
func (o asc) Less(i, j int) bool { return o[i].month < o[j].month }

// Basic search of cassandra.
// start inclusive, end exclusive
func searchCassandra(key string, start, end uint32) ([]Iter, error) {
	iters := make([]Iter, 0)
	if start > end {
		return iters, fmt.Errorf("start must be before end.")
	}

	results := make(chan outcome)
	wg := &sync.WaitGroup{}

	query := func(month uint32, q string, p ...interface{}) {
		wg.Add(1)
		go func() {
			//		if len(p) == 3 {
			//		log.Println("querying cassandra for", q, p[0], TS(p[1]), TS(p[2]))
			//		} else if len(p) == 2 {
			//		log.Println("querying cassandra for", q, p[0], TS(p[1]))
			//		} else {
			//		log.Println("querying cassandra for", q, p)
			//		}
			results <- outcome{month, cSession.Query(q, p...).Iter()}
			wg.Done()
		}()
	}

	start_month := start - (start % month_sec)       // starting row has to be at, or before, requested start
	end_month := (end - 1) - ((end - 1) % month_sec) // ending row has to be include the last point we might need

	pre := time.Now()
	if start_month == end_month {
		// we need a selection of the row between startTs and endTs
		row_key := fmt.Sprintf("%s_%d", key, start_month/month_sec)
		query(start_month, "SELECT ts, data FROM metric WHERE key = ? AND ts >= ? AND ts < ? ORDER BY ts ASC", row_key, start, end)
	} else {
		// get row_keys for each row we need to query.
		for month := start_month; month <= end_month; month += month_sec {
			row_key := fmt.Sprintf("%s_%d", key, month/month_sec)
			if month == start_month {
				// we want from startTs to the end of the row.
				query(month, "SELECT ts, data FROM metric WHERE key = ? AND ts >= ? ORDER BY ts ASC", row_key, start)
			} else if month == end_month {
				// we want from start of the row till the endTs.
				query(month, "SELECT ts, data FROM metric WHERE key = ? AND ts < ? ORDER BY ts ASC", row_key, end)
			} else {
				// we want all columns
				query(month, "SELECT ts, data FROM metric WHERE key = ? ORDER BY ts ASC", row_key)
			}
		}
	}
	outcomes := make([]outcome, 0)

	// wait for all queries to complete then close the results channel so that the following
	// for loop ends.
	go func() {
		wg.Wait()
		cassandraGetDuration.Value(time.Now().Sub(pre))
		close(results)
	}()

	for o := range results {
		outcomes = append(outcomes, o)
	}
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
				err := errors.New("unpossibly small chunk in cassandra")
				log.Error(3, err.Error())
				return iters, err
			}
			if Format(b[0]) != FormatStandardGoTsz {
				err := errors.New("unrecognized chunk format in cassandra")
				log.Error(3, err.Error())
				return iters, err
			}
			iter, err := tsz.NewIterator(b[1:])
			if err != nil {
				log.Error(3, "failed to unpack cassandra payload. %s", err)
				return iters, err
			}
			iters = append(iters, NewIter(iter, "cassandra month=%d t0=%d", outcome.month, ts))
		}
		cassandraChunksPerRow.Value(chunks)
		err := outcome.i.Close()
		if err != nil {
			log.Error(3, "cassandra query error. %s", err)
		}
	}
	cassandraRowsPerResponse.Value(int64(len(outcomes)))
	log.Debug("searchCassandra(): %d outcomes (queries), %d total iters", len(outcomes), len(iters))
	return iters, nil
}
