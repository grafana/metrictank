package input

import (
	"github.com/raintank/metrictank/idx"
	"github.com/raintank/metrictank/mdata"
	"github.com/raintank/metrictank/usage"
)

type Plugin interface {
	Name() string
	Start(metrics mdata.Metrics, metricIndex idx.MetricIndex, usg *usage.Usage)
	Stop() // Should block until shutdown is complete.
}
