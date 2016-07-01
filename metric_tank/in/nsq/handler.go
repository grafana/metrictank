package nsq

import (
	"github.com/nsqio/go-nsq"
	"github.com/raintank/met"
	"github.com/raintank/raintank-metric/metric_tank/defcache"
	"github.com/raintank/raintank-metric/metric_tank/in"
	"github.com/raintank/raintank-metric/metric_tank/mdata"
	"github.com/raintank/raintank-metric/metric_tank/usage"
)

type Handler struct {
	in.In
}

func NewHandler(metrics mdata.Metrics, defCache *defcache.DefCache, usg *usage.Usage, stats met.Backend) *Handler {
	return &Handler{
		In: in.New(metrics, defCache, usg, "nsq", stats),
	}
}

func (h *Handler) HandleMessage(m *nsq.Message) error {
	h.In.Handle(m.Body)
	return nil
}
