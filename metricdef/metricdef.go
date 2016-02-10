package metricdef

import (
	"github.com/raintank/raintank-metric/schema"
)

type Defs interface {
	GetMetrics(scroll_id string) ([]*schema.MetricDefinition, string, error)
	GetMetricDefinition(id string) (*schema.MetricDefinition, bool, error)
	IndexMetric(m *schema.MetricDefinition) error
	Stop()
	Clear() // only for unit testing
}
