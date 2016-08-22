package in

import (
	"github.com/raintank/metrictank/idx"
	"github.com/raintank/metrictank/mdata"
	"github.com/raintank/metrictank/usage"
)

type Plugin interface {
	Start(metrics mdata.Metrics, metricIndex idx.MetricIndex, usg *usage.Usage)
	Stop()
}
