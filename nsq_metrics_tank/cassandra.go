package main

import (
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/gocql/gocql"
)

// write aggregated data to cassandra.

/*
https://godoc.org/github.com/gocql/gocql#Session
Session is the interface used by users to interact with the database.
It's safe for concurrent use by multiple goroutines and a typical usage scenario is to have one global session
object to interact with the whole Cassandra cluster.
*/
var cSession *gocql.cSession

func InitCassandra() {
	cluster := gocql.NewCluster(cassandraAddrs)
	cluster.Keyspace = "raintank"
	cluster.Consistency = gocql.One
	cSession = cluster.CreateSession()
}

// Insert metric into Cassandra.
//
// key: is the metric_id
// ts: is the start of the aggregated time range.
// data: is the payload as bytes.
func InsertMetric(key string, ts time.Time, data []byte) error {
	query := "INSERT INTO metric (key, ts, data) values(?,?,?)"
	//get YYYYMM format of ts.
	row_ts := ts.Format("200601")
	row_key := fmt.Sprintf("%s_%s", key, row_ts)
	return cSession.Query(query, row_key, ts, data).Exec()
}
