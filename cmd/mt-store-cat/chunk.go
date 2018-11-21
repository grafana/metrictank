package main

import (
	"context"
	"fmt"
	"time"

	"github.com/grafana/metrictank/store/cassandra"
)

// printChunkSummary prints a summary of chunks in the store matching the given conditions, grouped in buckets of groupTTL size by their TTL
func printChunkSummary(ctx context.Context, store *cassandra.CassandraStore, tables []cassandra.Table, metrics []Metric, groupTTL string) error {
	now := uint32(time.Now().Unix())
	end_month := now - (now % cassandra.Month_sec)

	for _, tbl := range tables {
		// per store.FindExistingTables(), actual TTL may be up to 2x what's in tablename.
		// we query up to 4x so that we also include data that should have been dropped already but still sticks around for whatever reason.
		start := now - uint32(4*tbl.TTL*3600)
		start_month := start - (start % cassandra.Month_sec)
		fmt.Println("## Table", tbl.Name)
		if len(metrics) == 0 {
			query := fmt.Sprintf("select key, ttl(data) from %s", tbl.Name)
			iter := store.Session.Query(query).Iter()
			showKeyTTL(iter, groupTTL)
		} else {
			for _, metric := range metrics {
				for month := start_month; month <= end_month; month += cassandra.Month_sec {
					row_key := fmt.Sprintf("%s_%d", metric.AMKey.String(), month/cassandra.Month_sec)
					query := fmt.Sprintf("select key, ttl(data) from %s where key=?", tbl.Name)
					iter := store.Session.Query(query, row_key).Iter()
					showKeyTTL(iter, groupTTL)
				}
			}
		}
	}
	return nil
}
