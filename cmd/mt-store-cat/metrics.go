package main

import (
	"fmt"
	"path/filepath"
	"sort"
	"strings"

	"github.com/raintank/schema"
	log "github.com/sirupsen/logrus"

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

func match(prefix, substr, glob string, metric Metric) bool {
	if !strings.HasPrefix(metric.name, prefix) {
		return false
	}
	if !strings.Contains(metric.name, substr) {
		return false
	}
	if glob == "" {
		return true
	}

	// graphite separates by . -- Match separates by /
	glob2 := strings.Replace(glob, ".", "/", -1)
	name := strings.Replace(metric.name, ".", "/", -1)

	ok, err := filepath.Match(glob2, name)
	if err != nil {
		log.Fatalf("invalid match pattern %q: %s", glob, err)
	}
	return ok
}

// getMetrics lists all metrics from the store matching the given condition.
func getMetrics(store *cassandra.CassandraStore, prefix, substr, glob string, archive schema.Archive) ([]Metric, error) {
	var metrics []Metric
	iter := store.Session.Query("select id, name from metric_idx").Iter()
	var m Metric
	var idString string
	for iter.Scan(&idString, &m.name) {
		if match(prefix, substr, glob, m) {
			mkey, err := schema.MKeyFromString(idString)
			if err != nil {
				panic(err)
			}
			m.AMKey = schema.AMKey{
				MKey:    mkey,
				Archive: archive,
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
