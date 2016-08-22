package nsq

import (
	"github.com/nsqio/go-nsq"
	"github.com/raintank/met"
	"github.com/raintank/metrictank/idx"
	"github.com/raintank/metrictank/in"
	"github.com/raintank/metrictank/mdata"
	"github.com/raintank/metrictank/usage"
)

type Handler struct {
	in.In
}

func NewHandler(metrics mdata.Metrics, metricIndex idx.MetricIndex, usg *usage.Usage, stats met.Backend) *Handler {
	return &Handler{
		In: in.New(metrics, metricIndex, usg, "nsq", stats),
	}
}

func (h *Handler) HandleMessage(m *nsq.Message) error {
	h.In.HandleArray(m.Body)
	return nil
}
