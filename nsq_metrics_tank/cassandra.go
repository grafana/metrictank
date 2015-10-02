package main

import (
	"fmt"
	"log"
	"sort"
	"sync"

	"github.com/dgryski/go-tsz"
	"github.com/gocql/gocql"
)

// write aggregated data to cassandra.

const month = 60 * 60 * 24 * 28

const keyspace_schema = `CREATE KEYSPACE IF NOT EXISTS raintank WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}  AND durable_writes = true`
const table_schema = `CREATE TABLE IF NOT EXISTS raintank.metric (
    key ascii,
    ts int,
    data blob,
    PRIMARY KEY (key, ts)
) WITH COMPACT STORAGE
    AND CLUSTERING ORDER BY (ts DESC)`

/*
https://godoc.org/github.com/gocql/gocql#Session
Session is the interface used by users to interact with the database.
It's safe for concurrent use by multiple goroutines and a typical usage scenario is to have one global session
object to interact with the whole Cassandra cluster.
*/
var cSession *gocql.Session

func InitCassandra() error {
	cluster := gocql.NewCluster(cassandraAddrs...)
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
	cSession, err = cluster.CreateSession()
	return err
}

// Insert metric into Cassandra.
//
// key: is the metric_id
// ts: is the start of the aggregated time range.
// data: is the payload as bytes.
func InsertMetric(key string, t0 uint32, data []byte) error {
	// for unit tests
	if cSession == nil {
		return nil
	}
	query := "INSERT INTO metric (key, ts, data) values(?,?,?)"
	row_key := fmt.Sprintf("%s_%d", key, t0/month) // "month number" based on unix timestamp (rounded down)
	log.Println("saving chunk key", row_key, "timestamp", t0)
	return cSession.Query(query, row_key, t0, data).Exec()
}

type outcome struct {
	mark uint32
	i    *gocql.Iter
}
type asc []outcome

func (o asc) Len() int           { return len(o) }
func (o asc) Swap(i, j int)      { o[i], o[j] = o[j], o[i] }
func (o asc) Less(i, j int) bool { return o[i].mark < o[j].mark }

// Basic search of cassandra.
// start inclusive, end exclusive
func searchCassandra(key string, start, end uint32) ([]*tsz.Iter, error) {
	if start > end {
		panic(fmt.Sprintf("searchCassandra start %d > end %d", start, end))
	}

	results := make(chan outcome)
	wg := &sync.WaitGroup{}

	query := func(mark uint32, q string, p ...interface{}) {
		wg.Add(1)
		go func() {
			if len(p) == 3 {
				log.Println("querying cassandra for", q, p[0], TS(p[1]), TS(p[2]))
			} else if len(p) == 2 {
				log.Println("querying cassandra for", q, p[0], TS(p[1]))
			} else {
				log.Println("querying cassandra for", q, p)
			}
			results <- outcome{mark, cSession.Query(q, p...).Iter()}
			wg.Done()
		}()
	}

	start_month := start - (start % month)       // starting row has to be at, or before, requested start
	end_month := (end - 1) - ((end - 1) % month) // ending row has to be include the last point we might need

	if start_month == end_month {
		// we need a selection of the row between startTs and endTs
		row_key := fmt.Sprintf("%s_%d", key, start_month/month)
		query(start_month, "SELECT data FROM metric WHERE key = ? AND ts >= ? AND ts < ? ORDER BY ts ASC", row_key, start, end)
	} else {
		// get row_keys for each row we need to query.
		for mark := start_month; mark <= end_month; mark += month {
			row_key := fmt.Sprintf("%s_%d", key, mark/month)
			if mark == start_month {
				// we want from startTs to the end of the row.
				query(mark, "SELECT data FROM metric WHERE key = ? AND ts >= ? ORDER BY ts ASC", row_key, start)
			} else if mark == end_month {
				// we want from start of the row till the endTs.
				query(mark, "SELECT data FROM metric WHERE key = ? AND ts < ? ORDER BY ts ASC", row_key, end)
			} else {
				// we want all columns
				query(mark, "SELECT data FROM metric WHERE key = ? ORDER BY ts ASC", row_key)
			}
		}
	}
	outcomes := make([]outcome, 0)
	iters := make([]*tsz.Iter, 0)
	go func() {
		wg.Wait()
		close(results)
	}()
	for o := range results {
		outcomes = append(outcomes, o)
	}
	sort.Sort(asc(outcomes))

	var b []byte
	for _, outcome := range outcomes {
		for outcome.i.Scan(&b) {
			iter, err := tsz.NewIterator(b)
			if err != nil {
				log.Fatal(err)
			}
			iters = append(iters, iter)
		}
		err := outcome.i.Close()
		if err != nil {
			log.Println("ERROR:", err)
		}
	}
	log.Println(len(outcomes), "outcomes, cassandra results", len(iters))
	return iters, nil
}
