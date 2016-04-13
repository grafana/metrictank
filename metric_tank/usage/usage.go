package usage

import (
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
	period uint32
	now    map[int]orgstat
	prev   map[int]orgstat
}

func New(period uint32, m struc.Metrics, d *defcache.DefCache, cl clock.Clock) *Usage {
	metrics = m
	defCache = d
	Clock = cl
	ret := &Usage{
		period: period,
		now:    make(map[int]orgstat),
	}
	go ret.Report()
	return ret
}

func (u *Usage) Add(org int, key string) {
	u.Lock()
	if o, ok := u.now[org]; !ok {
		u.now[org] = orgstat{
			keys: map[string]struct{}{
				key: struct{}{},
			},
			points: 1,
		}
	} else {
		o.keys[key] = struct{}{}
		o.points += 1
		u.now[org] = o
	}
	u.Unlock()
}

func (u *Usage) Report() {
	// provides "clean" ticks at precise intervals, and delivers them shortly after
	tick := func() time.Time {
		now := Clock.Now()
		nowUnix := now.UnixNano()
		p := time.Duration(u.period) * time.Second
		diff := p - (time.Duration(nowUnix) % p)
		ideal := now.Add(diff)
		ticker := Clock.Ticker(diff)
		<-ticker.C
		return ideal
	}
	met := schema.MetricData{
		Interval: int(u.period),
		Tags:     []string{},
	}

	report := func(name, unit, tt string, val float64, met *schema.MetricData) {
		met.Name = name
		met.Metric = name
		met.Unit = unit
		met.TargetType = tt
		met.Value = val
		met.SetId()

		m := metrics.GetOrCreate(met.Id)
		m.Add(uint32(met.Time), met.Value)
		defCache.Add(met)
	}
	for {
		now := tick().Unix()
		u.Lock()
		u.prev = u.now
		u.now = make(map[int]orgstat)
		for i := range u.prev {
			u.now[i] = orgstat{
				keys:   make(map[string]struct{}),
				points: u.prev[i].points,
			}
		}
		u.Unlock()

		met.Time = now
		for org, stat := range u.prev {
			if org == -1 {
				// for the special case of org -1, meaning globally provided metrics visible to every org
				// let's provide this metric to the admin org which has id 1.
				// the reason we don't publish this with id -1 is that that would make it available to everyone
				// and confuse people about which metrics it counts
				met.OrgId = 1
				report("metric_tank.usage-minus1.numSeries", "metrics", "gauge", float64(len(stat.keys)), &met)
				report("metric_tank.usage-minus1.numPoints", "points", "counter", float64(stat.points), &met)
			} else {
				met.OrgId = org
				report("metric_tank.usage.numSeries", "metrics", "gauge", float64(len(stat.keys)), &met)
				report("metric_tank.usage.numPoints", "points", "counter", float64(stat.points), &met)
			}
		}
	}
}
