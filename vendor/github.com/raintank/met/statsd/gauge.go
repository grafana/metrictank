package statsd

import (
	"sync"
	"time"

	"github.com/raintank/met"
)

type Gauge struct {
	key string
	val int64
	sync.Mutex
	backend Backend
}

func (b Backend) NewGauge(key string, val int64) met.Gauge {
	g := Gauge{
		key:     key,
		backend: b,
	}
	go func() {
		for {
			g.Lock()
			g.backend.client.Gauge(g.key, int(g.val))
			g.Unlock()
			time.Sleep(time.Duration(1) * time.Second)
		}
	}()
	return &g
}

func (g *Gauge) Value(val int64) {
	g.Lock()
	g.val = val
	g.Unlock()
	g.backend.client.Gauge(g.key, int(val))
}

func (g *Gauge) Inc(val int64) {
	g.Lock()
	defer g.Unlock()
	g.val += val
	g.backend.client.Gauge(g.key, int(g.val))
}

func (g *Gauge) Dec(val int64) {
	g.Lock()
	defer g.Unlock()
	g.val -= val
	g.backend.client.Gauge(g.key, int(g.val))
}
