package main

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/grafana/metrictank/store/cassandra"
)

func chunkSummary(ctx context.Context, store *cassandra.CassandraStore, tables []string, metrics []Metric, keyspace, groupTTL string) error {
	now := uint32(time.Now().Unix())
	end_month := now - (now % cassandra.Month_sec)

	for _, tbl := range tables {
		// actual TTL may be up to 2x what's in tablename. see store/cassandra/store_cassandra.go for details
		// we query up to 4x so that we also include data that should have been dropped already but still sticks around for whatever reason.
		TTLHours, _ := strconv.Atoi(strings.Split(tbl, "_")[1])
		start := now - uint32(4*TTLHours*3600)
		start_month := start - (start % cassandra.Month_sec)
		fmt.Println("## Table", tbl)
		if len(metrics) == 0 {
			query := fmt.Sprintf("select key, ttl(data) from %s", tbl)
			iter := store.Session.Query(query).Iter()
			showKeyTTL(iter, groupTTL)
		} else {
			for _, metric := range metrics {
				for month := start_month; month <= end_month; month += cassandra.Month_sec {
					row_key := fmt.Sprintf("%s_%d", metric.id, month/cassandra.Month_sec)
					query := fmt.Sprintf("select key, ttl(data) from %s where key=?", tbl)
					iter := store.Session.Query(query, row_key).Iter()
					showKeyTTL(iter, groupTTL)
				}
			}
		}
	}
	return nil
}
