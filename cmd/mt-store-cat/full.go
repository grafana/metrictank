package main

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/raintank/metrictank/mdata"
)

type TablesByTTL []string

func (t TablesByTTL) Len() int      { return len(t) }
func (t TablesByTTL) Swap(i, j int) { t[i], t[j] = t[j], t[i] }
func (t TablesByTTL) Less(i, j int) bool {
	iTTL, _ := strconv.Atoi(strings.Split(t[i], "_")[1])
	jTTL, _ := strconv.Atoi(strings.Split(t[j], "_")[1])
	return iTTL < jTTL
}

type Metric struct {
	id   string
	name string
}

type MetricsByName []Metric

func (m MetricsByName) Len() int           { return len(m) }
func (m MetricsByName) Swap(i, j int)      { m[i], m[j] = m[j], m[i] }
func (m MetricsByName) Less(i, j int) bool { return m[i].name < m[j].name }

func Dump(keyspace, prefix string, store *mdata.CassandraStore, roundTTL int) error {
	var targets []Metric

	if prefix == "" {
		fmt.Println("# Looking for ALL metrics")
	} else {
		fmt.Println("# Looking for these metrics:")
		iter := store.Session.Query("select id,metric from metric_idx").Iter()
		var id, metric string
		for iter.Scan(&id, &metric) {
			if strings.HasPrefix(metric, prefix) {
				targets = append(targets, Metric{id, metric})
			}
		}
		err := iter.Close()
		if err != nil {
			log.Error(3, "cassandra query error. %s", err)
			return err
		}
		sort.Sort(MetricsByName(targets))
		for _, m := range targets {
			fmt.Println(m.id, m.name)
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

	sort.Sort(TablesByTTL(tables))
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
			for _, t := range targets {
				for month := start_month; month <= end_month; month++ {
					row_key := fmt.Sprintf("%s_%d", t.id, month/mdata.Month_sec)
					query := fmt.Sprintf("select key, ttl(data) from %s where key=?", tbl)
					iter := store.Session.Query(query, row_key).Iter()
					showKeyTTL(iter, roundTTL)
				}
			}
		}
	}
	return nil
}
