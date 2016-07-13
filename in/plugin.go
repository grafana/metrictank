package in

import (
	"github.com/raintank/metrictank/defcache"
	"github.com/raintank/metrictank/mdata"
	"github.com/raintank/metrictank/usage"
)

type Plugin interface {
	Start(metrics mdata.Metrics, defCache *defcache.DefCache, usg *usage.Usage)
	Stop()
}
