package main

import (
	"fmt"

	"github.com/gocql/gocql"
)

// write aggregated data to cassandra.

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
	row_key := fmt.Sprintf("%s_%d", key, (ts / 3600 / 24 / 28)) // "month number" based on unix timestamp (rounded down)
	return cSession.Query(query, row_key, ts, data).Exec()
}

// Basic search of cassandra.
// TODO:(Awoods) this needs to be refactored to run each query in a separate goroutine.
// then concatenate the results at the end.
func SearchCassandra(key string, start, end time.Time) ([]Point, error) {
	start_epoch := uint32(start.Unix())
	end_epoch := uint32(end.Unix())
	points := make([]Point, 0)
	//get row_keys we need to query.
	row_ts, err := MonthsInRange(start, end)
	if err != nil {
		return points, err
	}
	// we need to send a query for each row holding data.
	// this is 1 row per calender month.
	num_rows := len(row_ts)

	for i, ts := range row_ts {
		row_key := fmt.Sprintf("%s_%s", key, ts)
		var query string
		params := make([]interface{}, 0)

		// all queries need the row_key
		params = append(params, row_key)
		if num_rows == 1 {
			// we need a selection of the row between startTs and endTs
			query = "SELECT data FROM metric WHERE key = ? AND ts < ? AND ts >= ?"
			params = append(params, end, start)
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
			for iter.Next() {
				ts, val := iter.Values()
				if ts > start_epoch && ts <= end_epoch {
					points = append(points, Point{val, ts})
				}
			}
		}
	}
	return points, nil
}

func MonthsInRange(start, end time.Time) ([]string, error) {
	if start.After(end) {
		return nil, fmt.Errorf("start time after end time.")
	}
	months := make([]string, 0)

	year := start.Year()
	month := int(start.Month())

	if year != end.Year() {
		for month <= 12 {
			months = append(months, fmt.Sprintf("%d%02d", year, month))
			month++
		}
		//start new year.
		month = 1
		year++
	}

	for month <= int(end.Month()) {
		months = append(months, fmt.Sprintf("%d%02d", year, month))
		month++
	}
	return months, nil
}
