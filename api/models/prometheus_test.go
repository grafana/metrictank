package models

import (
	"fmt"
	"testing"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
)

func TestPrometheusSeriesSet(t *testing.T) {
	series1 := &PrometheusSeries{
		labels:  labels.FromStrings("bar", "foo"),
		samples: []model.SamplePair{{Value: 1, Timestamp: 2}},
	}
	series2 := &PrometheusSeries{
		labels:  labels.FromStrings("foo", "baz"),
		samples: []model.SamplePair{{Value: 3, Timestamp: 4}},
	}
	c := &PrometheusSeriesSet{
		series: []storage.Series{series1, series2},
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

func TestPrometheusIterator(t *testing.T) {
	s := &PrometheusSeries{
		labels:  labels.FromStrings("foo", "bar"),
		samples: []model.SamplePair{{Value: 1, Timestamp: 2}},
	}
	i := newPrometheusSeriesIterator(s)
	if !i.Seek(0) {
		t.Fatalf("seek(0) should result in data")
	}
	timestamp, val := i.At()
	if val != 1 || timestamp != 2 {
		t.Fatalf("Unexpected point (%d, %f) returned.", timestamp, val)
	}
	if !i.Seek(1) {
		fmt.Println(i.At())
		t.Fatalf("seek(1) should result in data")
	}
	if !i.Seek(2) {
		fmt.Println(i.At())
		t.Fatalf("seek(2) should result in data")
	}
	if i.Seek(3) {
		t.Fatalf("seek(3) should not result in data")
	}
}
