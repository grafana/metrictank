// Package metricdef provides the interface and implementations
// to interact with a metricdef[inition] index/database.
package metricdef

import (
	"gopkg.in/raintank/schema.v1"
)

type ResultCallback func(id string, ok bool)

type Defs interface {
	GetMetrics(scroll_id string) ([]*schema.MetricDefinition, string, error)
	GetMetricDefinition(id string) (*schema.MetricDefinition, bool, error)
	IndexMetric(m *schema.MetricDefinition) error
	SetAsyncResultCallback(fn ResultCallback) // asynchronous implementations use this to report the result back for each def id
	Stop()
}
