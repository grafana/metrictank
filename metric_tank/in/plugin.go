package in

import (
	"github.com/raintank/raintank-metric/metric_tank/defcache"
	"github.com/raintank/raintank-metric/metric_tank/mdata"
	"github.com/raintank/raintank-metric/metric_tank/usage"
)

type Plugin interface {
	Start(metrics mdata.Metrics, defCache *defcache.DefCache, usg *usage.Usage)
	Stop()
}
