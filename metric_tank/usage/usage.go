package usage

import (
	"fmt"
	"github.com/benbjohnson/clock"
	"github.com/raintank/raintank-metric/metric_tank/defcache"
	"github.com/raintank/raintank-metric/metric_tank/struc"
	"github.com/raintank/raintank-metric/schema"
	"sync"
	"time"
)

var Clock clock.Clock
var metrics struc.Metrics
var defCache *defcache.DefCache

type orgstat struct {
	keys   map[string]struct{} // track unique keys seen
	points uint32              // track amount of points seen
}

// tracks for every org
type Usage struct {
	sync.Mutex
	period time.Duration
	now    []orgstat
	prev   []orgstat
}

func New(t time.Duration, m struc.Metrics, d *defcache.DefCache, cl clock.Clock) *Usage {
	metrics = m
	defCache = d
	Clock = cl
	return &Usage{
		period: t,
	}
}

func (u *Usage) Add(org int, key string) {
	// org -1 is pos 0, etc.
	// org 5 -> pos 6 -> we need len 7 or more -> len should be >= pos+1
	pos := org + 1

	u.Lock()
	for len(u.now) < pos+1 {
		u.now = append(u.now, orgstat{
			keys: make(map[string]struct{}),
		})
	}
	u.now[pos].keys[key] = struct{}{}
	u.now[pos].points += 1
	u.Unlock()
}

func (u *Usage) Report() {
	// provides "clean" ticks at precise intervals, and delivers them shortly after
	tick := func() time.Time {
		now := Clock.Now()
		nowUnix := now.UnixNano()
		diff := u.period - (time.Duration(nowUnix) % u.period)
		ideal := now.Add(diff)
		ticker := Clock.Ticker(diff)
		<-ticker.C
		return ideal
	}
	met := schema.MetricData{
		Interval: 300,
		Tags:     []string{},
	}
	for {
		now := tick().Unix()
		u.Lock()
		u.prev = u.now
		u.now = make([]orgstat, len(u.prev))
		for i := range u.now {
			u.now[i].points = u.prev[i].points
		}
		u.Unlock()

		met.Time = now
		for pos, stat := range u.prev {
			met.OrgId = pos - 1

			met.Name = "metric_tank.usage.numSeries"
			met.Metric = met.Name
			met.Unit = "metrics"
			met.TargetType = "gauge"
			met.Value = float64(len(stat.keys))
			met.SetId()

			m := metrics.GetOrCreate(met.Id)
			m.Add(uint32(met.Time), met.Value)
			defCache.Add(&met)

			met.Name = "metric_tank.usage.numPoints"
			met.Metric = met.Name
			met.Unit = "points"
			met.TargetType = "counter"
			met.Value = float64(stat.points)
			met.SetId()

			m = metrics.GetOrCreate(met.Id)
			m.Add(uint32(met.Time), met.Value)
			defCache.Add(&met)
		}
	}
}
