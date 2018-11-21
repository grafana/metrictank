package main

import (
	"fmt"
	"sort"
	"strings"

	"github.com/raintank/schema"

	"github.com/grafana/metrictank/store/cassandra"
)

type Metric struct {
	AMKey schema.AMKey
	name  string
}

func (m Metric) String() string {
	return m.AMKey.String() + "(" + m.name + ")"
}

type MetricsByName []Metric

func (m MetricsByName) Len() int           { return len(m) }
func (m MetricsByName) Swap(i, j int)      { m[i], m[j] = m[j], m[i] }
func (m MetricsByName) Less(i, j int) bool { return m[i].name < m[j].name }

// prefix is optional
func getMetrics(store *cassandra.CassandraStore, prefix string) ([]Metric, error) {
	var metrics []Metric
	iter := store.Session.Query("select id, name from metric_idx").Iter()
	var m Metric
	var idString string
	for iter.Scan(&idString, &m.name) {
		if strings.HasPrefix(m.name, prefix) {
			mkey, err := schema.MKeyFromString(idString)
			if err != nil {
				panic(err)
			}
			m.AMKey = schema.AMKey{
				MKey: mkey,
			}
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

// getMetric returns the metric for the given AMKey
func getMetric(store *cassandra.CassandraStore, amkey schema.AMKey) ([]Metric, error) {
	var metrics []Metric
	// index only stores MKey's, not AMKey's.
	iter := store.Session.Query("select name from metric_idx where id=? ALLOW FILTERING", amkey.MKey.String()).Iter()
	var m Metric
	for iter.Scan(&m.name) {
		m.AMKey = amkey
		metrics = append(metrics, m)
	}
	if len(metrics) > 1 {
		panic(fmt.Sprintf("wtf. found more than one entry for id %v: %v", amkey, metrics))
	}
	err := iter.Close()
	if err != nil {
		return metrics, err
	}
	return metrics, nil
}
