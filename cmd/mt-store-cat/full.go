package main

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/raintank/metrictank/mdata"
)

func Dump(store *mdata.CassandraStore, tables []string, keyspace, prefix string, roundTTL int) error {
	var metrics []Metric
	var err error
	if prefix == "" {
		fmt.Println("# Looking for ALL metrics")
	} else {
		fmt.Println("# Looking for these metrics:")
		metrics, err = getMetrics(store, prefix)
		if err != nil {
			log.Error(3, "cassandra query error. %s", err)
			return err
		}
		for _, m := range metrics {
			fmt.Println(m.id, m.name)
		}
	}

	fmt.Printf("# Keyspace %q contents:\n", keyspace)

	now := uint32(time.Now().Unix())
	end_month := now - (now % mdata.Month_sec)

	for _, tbl := range tables {
		// actual TTL may be up to 2x what's in tablename. see mdata/store_cassandra.go for details
		// we query up to 4x so that we also include data that should have been dropped already but still sticks around for whatever reason.
		TTLHours, _ := strconv.Atoi(strings.Split(tbl, "_")[1])
		start := now - uint32(4*TTLHours)
		start_month := start - (start % mdata.Month_sec)
		fmt.Println("## Table", tbl)
		if prefix == "" {
			query := fmt.Sprintf("select key, ttl(data) from %s", tbl)
			iter := store.Session.Query(query).Iter()
			showKeyTTL(iter, roundTTL)
		} else {
			for _, metric := range metrics {
				for month := start_month; month <= end_month; month++ {
					row_key := fmt.Sprintf("%s_%d", metric.id, month/mdata.Month_sec)
					query := fmt.Sprintf("select key, ttl(data) from %s where key=?", tbl)
					iter := store.Session.Query(query, row_key).Iter()
					showKeyTTL(iter, roundTTL)
				}
			}
		}
	}
	return nil
}
