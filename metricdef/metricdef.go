package metricdef

import (
	"github.com/raintank/schema"
)

type ResultCallback func(id string, ok bool)

type Defs interface {
	GetMetrics(scroll_id string) ([]*schema.MetricDefinition, string, error)
	GetMetricDefinition(id string) (*schema.MetricDefinition, bool, error)
	IndexMetric(m *schema.MetricDefinition) error
	SetAsyncResultCallback(fn ResultCallback) // asynchronous implementations use this to report the result back for each def id
	Stop()
}
