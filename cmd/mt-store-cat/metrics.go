package main

import (
	"fmt"
	"sort"
	"strings"

	"github.com/raintank/metrictank/mdata"
)

type Metric struct {
	id   string
	name string
}

type MetricsByName []Metric

func (m MetricsByName) Len() int           { return len(m) }
func (m MetricsByName) Swap(i, j int)      { m[i], m[j] = m[j], m[i] }
func (m MetricsByName) Less(i, j int) bool { return m[i].name < m[j].name }

// prefix is optional
func getMetrics(store *mdata.CassandraStore, prefix string) ([]Metric, error) {
	var metrics []Metric
	iter := store.Session.Query("select id, metric from metric_idx").Iter()
	var m Metric
	for iter.Scan(&m.id, &m.name) {
		if strings.HasPrefix(m.name, prefix) {
			metrics = append(metrics, m)
		}
	}
	err := iter.Close()
	if err != nil {
		return metrics, err
	}
	sort.Sort(MetricsByName(metrics))
	return metrics, nil
}

func getMetric(store *mdata.CassandraStore, id string) ([]Metric, error) {
	var metrics []Metric
	iter := store.Session.Query("select id, metric from metric_idx where id=? ALLOW FILTERING", id).Iter()
	var m Metric
	for iter.Scan(&m.id, &m.name) {
		metrics = append(metrics, m)
	}
	if len(metrics) > 1 {
		panic(fmt.Sprintf("wtf. found more than one entry for id %q: %v", id, metrics))
	}
	err := iter.Close()
	if err != nil {
		return metrics, err
	}
	return metrics, nil
}
