package mdata

import (
	"gopkg.in/raintank/schema.v1"
)

// ReorderBuffer keeps a window of data during which it is ok to send data out of order.
// Once the reorder window has passed Add() will return the old data and delete it from the buffer.
// The reorder buffer itself is not thread safe because it is only used by AggMetric,
// which is thread safe, so there is no locking in the buffer.
type ReorderBuffer struct {
	len      uint32         // length of buffer in number of datapoints
	newest   uint32         // index of newest buffer entry
	interval uint32         // metric interval
	buf      []schema.Point // the actual buffer holding the data
}

func NewReorderBuffer(reorderWindow uint32, interval int) *ReorderBuffer {
	buf := &ReorderBuffer{
		len:      reorderWindow,
		interval: uint32(interval),
		buf:      make([]schema.Point, reorderWindow),
	}
	return buf
}

func (rob *ReorderBuffer) Add(ts uint32, val float64) ([]schema.Point, bool) {
	ts = AggBoundary(ts, rob.interval)

	// out of order and too old
	if rob.buf[rob.newest].Ts != 0 && ts <= rob.buf[rob.newest].Ts-(rob.len*rob.interval) {
		metricsTooOld.Inc()
		return nil, false
	}

	var res []schema.Point
	oldest := (rob.newest + 1) % rob.len
	index := (ts / rob.interval) % rob.len
	if ts > rob.buf[rob.newest].Ts {
		flushCount := (ts - rob.buf[rob.newest].Ts) / rob.interval
		if flushCount > rob.len {
			flushCount = rob.len
		}

		for i := uint32(0); i < flushCount; i++ {
			if rob.buf[oldest].Ts != 0 {
				res = append(res, rob.buf[oldest])
			}
			rob.buf[oldest].Ts = 0
			rob.buf[oldest].Val = 0
			oldest = (oldest + 1) % rob.len
		}
		rob.buf[index].Ts = ts
		rob.buf[index].Val = val
		rob.newest = index
	} else {
		metricsReordered.Inc()
		rob.buf[index].Ts = ts
		rob.buf[index].Val = val
	}

	return res, true
}

// returns all the data in the buffer as a raw list of points
func (rob *ReorderBuffer) Get() []schema.Point {
	res := make([]schema.Point, 0, rob.len)
	oldest := (rob.newest + 1) % rob.len

	for {
		if rob.buf[oldest].Ts != 0 {
			res = append(res, rob.buf[oldest])
		}
		if oldest == rob.newest {
			break
		}
		oldest = (oldest + 1) % rob.len
	}

	return res
}

func (rob *ReorderBuffer) Reset() {
	for i := range rob.buf {
		rob.buf[i].Ts = 0
		rob.buf[i].Val = 0
	}
	rob.newest = 0
}

func (rob *ReorderBuffer) Flush() []schema.Point {
	res := rob.Get()
	rob.Reset()

	return res
}

func (rob *ReorderBuffer) IsEmpty() bool {
	return rob.buf[rob.newest].Ts == 0
}
