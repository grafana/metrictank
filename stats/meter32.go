package stats

import (
	"math"
	"sort"
	"sync"
	"time"

	"github.com/dgryski/go-linlog"
)

// meter maintains a histogram, from which it reports summary statistics such as quantiles.
// you can choose to approximate, in which case it uses linear-log bucketed class boundaries
// (we could also report the bins but right now we wouldn't be able to do anything with them
// and also the boundaries are powers of two which is a bit weird)

type Meter32 struct {
	approx bool

	sync.Mutex
	hist  map[uint32]uint32
	min   uint32
	max   uint32
	count uint32
	since time.Time
}

func NewMeter32(name string, approx bool) *Meter32 {
	return registry.getOrAdd(name, &Meter32{
		approx: approx,
		hist:   make(map[uint32]uint32),
		min:    math.MaxUint32,
		since:  time.Now(),
	},
	).(*Meter32)
}

func (m *Meter32) clear() {
	m.hist = make(map[uint32]uint32)
	m.min = math.MaxUint32
	m.max = 0
	m.count = 0
}

func (m *Meter32) Value(val int) {
	m.ValueUint32(uint32(val))
}

func (m *Meter32) ValueUint32(val uint32) {
	bin := val
	if m.approx {
		// subbin log2(16)= 4 -> up to 100/16 = 6.25% error I think
		// in practice it's max about 12% but anyway.
		tmp, _ := linlog.BinOf(uint64(val), 4, 2)
		bin = uint32(tmp)
	}
	m.Lock()
	if val < m.min {
		m.min = val
	}

	if val > m.max {
		m.max = val
	}
	m.hist[bin]++
	m.count += 1
	m.Unlock()
}

func (m *Meter32) ReportGraphite(prefix, buf []byte, now time.Time) []byte {
	m.Lock()
	if m.count == 0 {
		m.Unlock()
		return buf
	}
	keys := make([]int, 0, len(m.hist))
	for k := range m.hist {
		keys = append(keys, int(k))
	}
	sort.Ints(keys)

	quantiles := []struct {
		p   float64
		str string
	}{
		{0.50, "median.gauge32"},
		{0.75, "p75.gauge32"},
		{0.90, "p90.gauge32"},
	}

	pidx := 0
	runningcount := uint32(0)
	runningsum := uint64(0)

	for _, k := range keys {
		key := uint32(k)
		runningcount += m.hist[key]
		runningsum += uint64(m.hist[key]) * uint64(key)
		p := float64(runningcount) / float64(m.count)
		for pidx < len(quantiles) && quantiles[pidx].p <= p {
			buf = WriteUint32(buf, prefix, []byte(quantiles[pidx].str), key, now)
			pidx++
		}
	}

	buf = WriteUint32(buf, prefix, []byte("min.gauge32"), m.min, now)
	buf = WriteUint32(buf, prefix, []byte("mean.gauge32"), uint32(runningsum/uint64(m.count)), now)
	buf = WriteUint32(buf, prefix, []byte("max.gauge32"), m.max, now)
	buf = WriteUint32(buf, prefix, []byte("values.count32"), m.count, now)
	buf = WriteFloat64(buf, prefix, []byte("values.rate32"), float64(m.count)/now.Sub(m.since).Seconds(), now)
	m.since = now

	m.clear()
	m.Unlock()

	return buf
}
