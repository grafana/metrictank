package mdata

import (
	"gopkg.in/raintank/schema.v1"
)

/*
 * The write buffer keeps a window of data during which it is ok to send data out of order.
 * Once the reorder window has passed it will try to flush the data out.
 * The write buffer itself is not thread safe because it is only used by AggMetric,
 * which is thread safe, so there is no locking in the buffer.
 */

type ReorderBuffer struct {
	len      uint32                // length of buffer in number of datapoints
	newest   uint32                // index of newest buffer entry
	interval uint32                // metric interval
	buf      []schema.Point        // the actual buffer holding the data
	flush    func(uint32, float64) // flushCount callback
}

func NewReorderBuffer(reorderWindow uint32, interval int, flush func(uint32, float64)) *ReorderBuffer {
	buf := &ReorderBuffer{
		len:      reorderWindow,
		flush:    flush,
		interval: uint32(interval),
		newest:   0,
		buf:      make([]schema.Point, reorderWindow),
	}
	return buf
}

func (wb *ReorderBuffer) Add(ts uint32, val float64) bool {
	ts = aggBoundary(ts, wb.interval)

	// out of order and too old
	if wb.buf[wb.newest].Ts != 0 && ts <= wb.buf[wb.newest].Ts-(wb.len*wb.interval) {
		return false
	}

	oldest := (wb.newest + 1) % wb.len
	index := (ts / wb.interval) % wb.len
	if ts > wb.buf[wb.newest].Ts {
		flushCount := (ts - wb.buf[wb.newest].Ts) / wb.interval
		if flushCount > wb.len {
			flushCount = wb.len
		}

		for i := uint32(0); i < flushCount; i++ {
			if wb.buf[oldest].Ts != 0 {
				wb.flush(wb.buf[oldest].Ts, wb.buf[oldest].Val)
			}
			wb.buf[oldest].Ts = 0
			wb.buf[oldest].Val = 0
			oldest = (oldest + 1) % wb.len
		}
		wb.buf[index].Ts = ts
		wb.buf[index].Val = val
		wb.newest = index
	} else {
		wb.buf[index].Ts = ts
		wb.buf[index].Val = val
	}

	return true
}

// returns all the data in the buffer as a raw list of points
func (wb *ReorderBuffer) Get() []schema.Point {
	res := make([]schema.Point, 0, wb.len)
	oldest := (wb.newest + 1) % wb.len

	for {
		if wb.buf[oldest].Ts != 0 {
			res = append(res, wb.buf[oldest])
		}
		if oldest == wb.newest {
			break
		}
		oldest = (oldest + 1) % wb.len
	}

	return res
}
