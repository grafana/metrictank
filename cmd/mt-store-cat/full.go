package main

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	log "github.com/Sirupsen/logrus"
	"github.com/raintank/metrictank/mdata"
)

type byTTL []string

func (t byTTL) Len() int      { return len(t) }
func (t byTTL) Swap(i, j int) { t[i], t[j] = t[j], t[i] }
func (t byTTL) Less(i, j int) bool {
	iTTL, _ := strconv.Atoi(strings.Split(t[i], "_")[1])
	jTTL, _ := strconv.Atoi(strings.Split(t[j], "_")[1])
	return iTTL < jTTL
}

func Dump(keyspace, prefix string, store *mdata.CassandraStore, roundTTL int) error {
	targets := make(map[string]string)

	if prefix == "" {
		fmt.Println("# Looking for ALL metrics")
	} else {
		fmt.Println("# Looking for these metrics:")
		iter := store.Session.Query("select id,metric from metric_idx").Iter()
		var id, metric string
		for iter.Scan(&id, &metric) {
			if strings.HasPrefix(metric, prefix) {
				fmt.Println(id, metric)
				targets[id] = metric
			}
		}
		err := iter.Close()
		if err != nil {
			log.Error(3, "cassandra query error. %s", err)
			return err
		}
	}

	fmt.Printf("# Keyspace %q contents:\n", keyspace)

	meta, err := store.Session.KeyspaceMetadata(keyspace)
	if err != nil {
		return err
	}
	var tables []string
	for tbl := range meta.Tables {
		if tbl == "metric_idx" || !strings.HasPrefix(tbl, "metric_") {
			continue
		}
		tables = append(tables, tbl)
	}

	sort.Sort(byTTL(tables))
	for _, tbl := range tables {
		query := fmt.Sprintf("select key, ttl(data) from %s", tbl)
		//	ret := c.session.Query(query, row_key, t0, data).Exec()
		fmt.Println("## Table", tbl)
		iter := store.Session.Query(query).Iter()
		var row, prevRow string
		var ttl, prevTTL, cnt int
		for iter.Scan(&row, &ttl) {
			rowParts := strings.Split(row, "_")
			if prefix != "" {
				_, ok := targets[rowParts[0]]
				if !ok {
					continue
				}
			}
			ttl = ttl / roundTTL
			if ttl == prevTTL && row == prevRow {
				cnt += 1
			} else {
				if prevRow != "" && prevTTL != 0 {
					fmt.Println(prevRow, prevTTL, cnt)
				}
				cnt = 0
				prevTTL = ttl
				prevRow = row
			}
		}
		if cnt != 0 {
			fmt.Println(prevRow, prevTTL, cnt)
		}
		err := iter.Close()
		if err != nil {
			log.Error(3, "cassandra query error. %s", err)
		}
	}
	return nil
}
