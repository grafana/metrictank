package main

import (
	"context"
	"fmt"
	"time"

	"github.com/grafana/metrictank/pkg/store/cassandra"
	log "github.com/sirupsen/logrus"
)

// printChunkSummary prints a summary of chunks in the store matching the given conditions, grouped in buckets of groupTTL size by their TTL
func printChunkSummary(ctx context.Context, store *cassandra.CassandraStore, tables []cassandra.Table, metrics []Metric, groupTTL string) error {
	now := uint32(time.Now().Unix())
	endMonth := now / cassandra.Month_sec

	for _, tbl := range tables {
		// per store.FindExistingTables(), actual TTL may be up to 2x what's in tablename.
		// we query up to 4x so that we also include data that should have been dropped already but still sticks around for whatever reason.
		start := now - uint32(4*tbl.TTL*3600)
		startMonth := start / cassandra.Month_sec
		fmt.Println("## Table", tbl.Name)
		if len(metrics) == 0 {
			query := fmt.Sprintf("select key, ttl(data) from %s", tbl.Name)
			session := store.Session.CurrentSession()
			iter := session.Query(query).Iter()
			showKeyTTL(iter, groupTTL)
		} else {
			for _, metric := range metrics {
				for num := startMonth; num <= endMonth; num += 1 {
					row_key := fmt.Sprintf("%s_%d", metric.AMKey.String(), num)
					query := fmt.Sprintf("select key, ttl(data) from %s where key=?", tbl.Name)
					session := store.Session.CurrentSession()
					iter := session.Query(query, row_key).Iter()
					showKeyTTL(iter, groupTTL)
				}
			}
		}
	}
	return nil
}

func printChunkCsv(ctx context.Context, store *cassandra.CassandraStore, table cassandra.Table, metrics []Metric, start, end uint32) {

	// see CassandraStore.SearchTable for more information
	startMonth := start / cassandra.Month_sec   // starting row has to be at, or before, requested start
	endMonth := (end - 1) / cassandra.Month_sec // ending row has to include the last point we might need (end-1)
	rowKeys := make([]string, endMonth-startMonth+1)

	query := fmt.Sprintf("SELECT key, ts, data FROM %s WHERE key IN ? AND ts < ?", table.Name)

	for _, metric := range metrics {
		i := 0
		for num := startMonth; num <= endMonth; num += 1 {
			rowKeys[i] = fmt.Sprintf("%s_%d", metric.AMKey.String(), num)
			i++
		}
		params := []interface{}{rowKeys, end}
		session := store.Session.CurrentSession()
		iter := session.Query(query, params...).WithContext(ctx).Iter()
		var key string
		var ts int
		var b []byte
		for iter.Scan(&key, &ts, &b) {
			fmt.Printf("%s,%d,0x%x\n", key, ts, b)
		}
		err := iter.Close()
		if err != nil {
			if err == context.Canceled || err == context.DeadlineExceeded {
				log.Fatal("query was aborted")
			}
			log.Fatalf("query failure: %v", err)
		}
	}
}
