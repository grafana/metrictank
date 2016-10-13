// Package usage is the component that tracks and reports how many series and points
// are being ingested for each organisation.
package usage

import (
	"sync"
	"time"

	"github.com/raintank/metrictank/idx"
	"github.com/raintank/metrictank/mdata"
	"gopkg.in/raintank/schema.v1"
)

var metrics mdata.Metrics
var metricIndex idx.MetricIndex

type orgstat struct {
	keys   map[string]struct{} // track unique keys seen
	points uint32              // track amount of points seen
}

// tracks for every org
type Usage struct {
	sync.Mutex
	period  time.Duration
	seconds int
	now     map[int]orgstat
	prev    map[int]orgstat
	stop    chan struct{}
}

// New creates a new Usage reporter, reporting every period
// secs controls the second-level interval of the output metrics
// it's up to the caller to set this in accordance to period
// we could deduce this from period using int(period.Seconds()) but I'm
// concerned of float-to-int conversion rounding errors
// for unit testing, the value of secs doesn't really matter
func New(period time.Duration, secs int, m mdata.Metrics, i idx.MetricIndex) *Usage {
	metrics = m
	metricIndex = i
	ret := &Usage{
		period:  period,
		seconds: secs,
		now:     make(map[int]orgstat),
		stop:    make(chan struct{}),
	}
	go ret.Report()
	return ret
}

func (u *Usage) Stop() {
	u.stop <- struct{}{}
}

func (u *Usage) Add(org int, key string) {
	u.Lock()
	if o, ok := u.now[org]; !ok {
		u.now[org] = orgstat{
			keys: map[string]struct{}{
				key: {},
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

// a bit of a hack only for package-internal use (e.g. testing) to manipulate the internal counter
func (u *Usage) set(org int, key string, points uint32) {
	u.Lock()
	o := u.now[org]
	o.keys[key] = struct{}{}
	o.points = points
	u.now[org] = o
	u.Unlock()
}

func (u *Usage) Report() {
	// provides "clean" ticks at precise intervals, and delivers them shortly after
	tick := func() chan time.Time {
		now := time.Now()
		nowUnix := now.UnixNano()
		diff := u.period - (time.Duration(nowUnix) % u.period)
		ideal := now.Add(diff)
		ch := make(chan time.Time)
		go func() {
			time.Sleep(diff)
			ch <- ideal
		}()
		return ch
	}
	met := schema.MetricData{
		Interval: u.seconds,
		Tags:     []string{},
	}

	report := func(name, unit, mtype string, val float64, met *schema.MetricData) {
		met.Name = name
		met.Metric = name
		met.Unit = unit
		met.Mtype = mtype
		met.Value = val
		met.SetId()

		m := metrics.GetOrCreate(met.Id)
		m.Add(uint32(met.Time), met.Value)
		metricIndex.Add(met)
	}
	for {
		ticker := tick()
		var now time.Time
		select {
		case <-u.stop:
			return
		case now = <-ticker:
		}
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

		met.Time = now.Unix()
		for org, stat := range u.prev {
			if org == -1 {
				// for the special case of org -1, meaning globally provided metrics visible to every org
				// let's provide this metric to the admin org which has id 1.
				// the reason we don't publish this with id -1 is that that would make it available to everyone
				// and confuse people about which metrics it counts
				met.OrgId = 1
				report("metrictank.usage-minus1.numSeries", "serie", "gauge", float64(len(stat.keys)), &met)
				report("metrictank.usage-minus1.numPoints", "point", "counter", float64(stat.points), &met)
			} else {
				met.OrgId = org
				report("metrictank.usage.numSeries", "serie", "gauge", float64(len(stat.keys)), &met)
				report("metrictank.usage.numPoints", "point", "counter", float64(stat.points), &met)
			}
		}
	}
}
