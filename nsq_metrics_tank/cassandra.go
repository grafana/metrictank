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
