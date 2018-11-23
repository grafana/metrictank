package main

import (
	"fmt"
	"sort"
	"strings"

	"github.com/grafana/metrictank/store/cassandra"
	log "github.com/sirupsen/logrus"
)

type TablesByTTL []cassandra.Table

func (t TablesByTTL) Len() int           { return len(t) }
func (t TablesByTTL) Swap(i, j int)      { t[i], t[j] = t[j], t[i] }
func (t TablesByTTL) Less(i, j int) bool { return t[i].TTL < t[j].TTL }

// getTables returns the requested cassandra store tables in TTL asc order based on match string
func getTables(store *cassandra.CassandraStore, match string) ([]cassandra.Table, error) {
	var tables []cassandra.Table
	if match == "*" || match == "" {
		for _, table := range store.TTLTables {
			if table.Name == "metric_idx" || !strings.HasPrefix(table.Name, "metric_") {
				continue
			}
			tables = append(tables, table)
		}
		sort.Sort(TablesByTTL(tables))
	} else {
		for _, table := range store.TTLTables {
			if table.Name == match {
				tables = append(tables, table)
				return tables, nil
			}
		}
		return nil, fmt.Errorf("table %q not found", match)
	}
	return tables, nil
}

//printTables prints all tables in the store
func printTables(store *cassandra.CassandraStore) {
	tables, err := getTables(store, "")
	if err != nil {
		log.Fatal(err.Error())
	}
	for _, table := range tables {
		fmt.Printf("%s (%d hours <= ttl < %d hours)\n", table.Name, table.TTL, table.TTL*2)
	}
}
