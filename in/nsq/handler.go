package nsq

import (
	"github.com/nsqio/go-nsq"
	"github.com/raintank/met"
	"github.com/raintank/metrictank/defcache"
	"github.com/raintank/metrictank/in"
	"github.com/raintank/metrictank/mdata"
	"github.com/raintank/metrictank/usage"
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
	h.In.HandleArray(m.Body)
	return nil
}
