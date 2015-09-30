package main

import (
	"fmt"
	"log"

	"github.com/dgryski/go-tsz"
	"github.com/gocql/gocql"
)

// write aggregated data to cassandra.

const month = 60 * 60 * 24 * 28

/*
https://godoc.org/github.com/gocql/gocql#Session
Session is the interface used by users to interact with the database.
It's safe for concurrent use by multiple goroutines and a typical usage scenario is to have one global session
object to interact with the whole Cassandra cluster.
*/
var cSession *gocql.Session

func InitCassandra() error {
	cluster := gocql.NewCluster(cassandraAddrs...)
	cluster.Keyspace = "raintank"
	cluster.Consistency = gocql.One
	var err error
	cSession, err = cluster.CreateSession()
	return err
}

// Insert metric into Cassandra.
//
// key: is the metric_id
// ts: is the start of the aggregated time range.
// data: is the payload as bytes.
func InsertMetric(key string, ts uint32, data []byte) error {
	query := "INSERT INTO metric (key, ts, data) values(?,?,?)"
	row_key := fmt.Sprintf("%s_%d", key, ts/month) // "month number" based on unix timestamp (rounded down)
	return cSession.Query(query, row_key, ts, data).Exec()
}

// Basic search of cassandra.
// TODO:(Awoods) this needs to be refactored to run each query in a separate goroutine.
// then concatenate the results at the end.
// TODO fine tune the details (inclusive/exclusive, make sure we don't query for an extra month if we don't need to)
func searchCassandra(key string, start, end uint32) ([]*tsz.Iter, error) {
	iters := make([]*tsz.Iter, 0)
	// we need to send a query for each row holding data
	// get row_keys we need to query.
	row_keys := make([]string, 0)
	start_month := start - (start % month)   // start has to be at, or before, requested start
	end_month := end - (end % month) + month // end has to be at, or after requested end
	for mark := start_month; mark <= end_month; mark += month {
		row_keys = append(row_keys, fmt.Sprintf("%s_%d", key, mark))
	}
	num_rows := len(row_keys)

	for i, row_key := range row_keys {
		var query string
		params := make([]interface{}, 0)

		// all queries need the row_key
		params = append(params, row_key)
		if num_rows == 1 {
			// we need a selection of the row between startTs and endTs
			query = "SELECT data FROM metric WHERE key = ? AND ts >= ? AND ts < ?"
			params = append(params, start, end)
		} else if i < 1 {
			// we want from startTs to the end of the row.
			query = "SELECT data FROM metric WHERE key = ? AND ts >= ?"
			params = append(params, start)
		} else {
			// we want from start of the row till the endTs.
			query = "SELECT data FROM metric WHERE key = ? AND ts < ?"
			params = append(params, end)
		}
		var b []byte
		result := cSession.Query(query, params...).Iter()
		for result.Scan(&b) {
			iter, err := tsz.NewIterator(b)
			if err != nil {
				log.Fatal(err)
			}
			iters = append(iters, iter)
		}
	}
	return iters, nil
}
