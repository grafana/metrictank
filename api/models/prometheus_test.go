package models

import (
	"testing"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
)

func TestPrometheusSeriesSet(t *testing.T) {
	series1 := &PrometheusSeries{
		labels:  labels.FromStrings("foo", "bar"),
		samples: []model.SamplePair{{Value: 1, Timestamp: 2}},
	}
	series2 := &PrometheusSeries{
		labels:  labels.FromStrings("foo", "baz"),
		samples: []model.SamplePair{{Value: 3, Timestamp: 4}},
	}
	c := &PrometheusSeriesSet{
		series: []storage.Series{series1, series2},
	}
	if !c.Next() {
		t.Fatalf("Expected Next() to be true.")
	}
	if c.At() != series1 {
		t.Fatalf("Unexpected series returned.")
	}
	if !c.Next() {
		t.Fatalf("Expected Next() to be true.")
	}
	if c.At() != series2 {
		t.Fatalf("Unexpected series returned.")
	}
	if c.Next() {
		t.Fatalf("Expected Next() to be false.")
	}
}
