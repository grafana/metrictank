package main

import (
	"sort"
	"strings"

	"github.com/raintank/schema"

	"github.com/grafana/metrictank/store/cassandra"
	log "github.com/sirupsen/logrus"
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
	iter := store.Session.Query("select id, metric from metric_idx").Iter()
	var m Metric
	var idString string
	for iter.Scan(&idString, &m.name) {
		if strings.HasPrefix(m.name, prefix) {
			mkey, err := schema.MKeyFromString(idString)
			if err != nil {
				log.WithFields(log.Fields{
					"error": err.Error(),
					"id":    idString,
				}).Panic("failed to get mkey from id")
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

func getMetric(store *cassandra.CassandraStore, amkey schema.AMKey) ([]Metric, error) {
	var metrics []Metric
	// index only stores MKey's, not AMKey's.
	iter := store.Session.Query("select id, metric from metric_idx where id=? ALLOW FILTERING", amkey.MKey).Iter()
	var m Metric
	var idString string
	for iter.Scan(idString, &m.name) {
		mkey, err := schema.MKeyFromString(idString)
		if err != nil {
			log.WithFields(log.Fields{
				"error": err.Error(),
				"id":    idString,
			}).Panic("failed to get mkey from id")
		}
		m.AMKey = schema.AMKey{
			MKey:    mkey,
			Archive: amkey.Archive,
		}
		metrics = append(metrics, m)
	}
	if len(metrics) > 1 {
		log.WithFields(log.Fields{
			"id":      amkey,
			"metrics": metrics,
		}).Panic("found more than one entry for id")
	}
	err := iter.Close()
	if err != nil {
		return metrics, err
	}
	return metrics, nil
}
