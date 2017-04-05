package main

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/raintank/metrictank/mdata"
)

func chunkSummary(store *mdata.CassandraStore, tables []string, metrics []Metric, keyspace, groupTTL string) error {
	now := uint32(time.Now().Unix())
	end_month := now - (now % mdata.Month_sec)

	for _, tbl := range tables {
		// actual TTL may be up to 2x what's in tablename. see mdata/store_cassandra.go for details
		// we query up to 4x so that we also include data that should have been dropped already but still sticks around for whatever reason.
		TTLHours, _ := strconv.Atoi(strings.Split(tbl, "_")[1])
		start := now - uint32(4*TTLHours*3600)
		start_month := start - (start % mdata.Month_sec)
		fmt.Println("## Table", tbl)
		if len(metrics) == 0 {
			query := fmt.Sprintf("select key, ttl(data) from %s", tbl)
			iter := store.Session.Query(query).Iter()
			showKeyTTL(iter, groupTTL)
		} else {
			for _, metric := range metrics {
				for month := start_month; month <= end_month; month += mdata.Month_sec {
					row_key := fmt.Sprintf("%s_%d", metric.id, month/mdata.Month_sec)
					query := fmt.Sprintf("select key, ttl(data) from %s where key=?", tbl)
					iter := store.Session.Query(query, row_key).Iter()
					showKeyTTL(iter, groupTTL)
				}
			}
		}
	}
	return nil
}
