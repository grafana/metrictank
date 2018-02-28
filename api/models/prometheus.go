package models

import (
	"sort"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
)

type PrometheusRangeQuery struct {
	Query   string `form:"query"`   //<string>: Prometheus expression query string.
	Start   string `form:"start"`   //<rfc3339 | unix_timestamp>: Start timestamp.
	End     string `form:"end"`     //<rfc3339 | unix_timestamp>: End timestamp.
	Step    string `form:"step"`    //<duration>: Query resolution step width.
	Timeout string `form:"timeout"` //<duration>: Evaluation timeout. Optional. Defaults to and is capped by the value of the -query.timeout flag.
}

type PrometheusQueryInstant struct {
	Query   string `form:"query"`   //<string>: Prometheus expression query string.
	Time    string `form:"time"`    //<rfc3339 | unix_timestamp>: Start timestamp.
	Timeout string `form:"timeout"` //<duration>: Evaluation timeout. Optional. Defaults to and is capped by the value of the -query.timeout flag.
}

type PrometheusSeriesQuery struct {
	Match []string `form:"match[]"` //<string>: Prometheus expression query string.
	Start string   `form:"start"`   //<rfc3339 | unix_timestamp>: Start timestamp.
	End   string   `form:"end"`     //<rfc3339 | unix_timestamp>: End timestamp.
}

type PrometheusSeriesSet struct {
	cur    int
	series []storage.Series
}

func NewPrometheusSeriesSet(series []storage.Series) *PrometheusSeriesSet {
	return &PrometheusSeriesSet{
		cur:    -1,
		series: series,
	}
}

func (p *PrometheusSeriesSet) Next() bool {
	p.cur++
	return p.cur < len(p.series)
}

func (p *PrometheusSeriesSet) At() storage.Series {
	return p.series[p.cur]
}

func (p *PrometheusSeriesSet) Err() error {
	return nil
}

type PrometheusSeries struct {
	labels  labels.Labels
	samples []model.SamplePair
}

func NewPrometheusSeries(tags map[string]string, samples []model.SamplePair) *PrometheusSeries {
	return &PrometheusSeries{
		labels:  labels.FromMap(tags),
		samples: samples,
	}
}

func (p *PrometheusSeries) Labels() labels.Labels {
	return p.labels
}

func (p *PrometheusSeries) Iterator() storage.SeriesIterator {
	return newPrometheusSeriesIterator(p)
}

type PrometheusSeriesIterator struct {
	cur    int
	series *PrometheusSeries
}

func newPrometheusSeriesIterator(series *PrometheusSeries) storage.SeriesIterator {
	return &PrometheusSeriesIterator{
		cur:    -1,
		series: series,
	}
}

func (c *PrometheusSeriesIterator) Seek(t int64) bool {
	c.cur = sort.Search(len(c.series.samples), func(n int) bool {
		return c.series.samples[n].Timestamp >= model.Time(t)
	})
	return c.cur < len(c.series.samples)
}

func (c *PrometheusSeriesIterator) At() (t int64, v float64) {
	s := c.series.samples[c.cur]
	return int64(s.Timestamp), float64(s.Value)
}

func (c *PrometheusSeriesIterator) Next() bool {
	c.cur++
	return c.cur < len(c.series.samples)
}

func (c *PrometheusSeriesIterator) Err() error {
	return nil
}
