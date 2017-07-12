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

type WriteBuffer struct {
	len      uint32                // length of buffer in number of datapoints
	newest   uint32                // index of newest buffer entry
	interval uint32                // metric interval
	buf      []entry               // the actual buffer holding the data
	flush    func(uint32, float64) // flushCount callback
}

type entry struct {
	ts  uint32
	val float64
}

func NewWriteBuffer(reorderWindow uint32, interval int, flush func(uint32, float64)) *WriteBuffer {
	buf := &WriteBuffer{
		len:      reorderWindow,
		flush:    flush,
		interval: uint32(interval),
		newest:   0,
		buf:      make([]entry, reorderWindow),
	}
	return buf
}

func (wb *WriteBuffer) Add(ts uint32, val float64) bool {
	ts = aggBoundary(ts, wb.interval)

	// out of order and too old
	if wb.buf[wb.newest].ts != 0 && ts <= wb.buf[wb.newest].ts-(wb.len*wb.interval) {
		return false
	}

	oldest := (wb.newest + 1) % wb.len
	index := (ts / wb.interval) % wb.len
	if ts > wb.buf[wb.newest].ts {
		flushCount := (ts - wb.buf[wb.newest].ts) / wb.interval
		if flushCount > wb.len {
			flushCount = wb.len
		}

		for i := uint32(0); i < flushCount; i++ {
			if wb.buf[oldest].ts != 0 {
				wb.flush(wb.buf[oldest].ts, wb.buf[oldest].val)
			}
			wb.buf[oldest].ts = 0
			wb.buf[oldest].val = 0
			oldest = (oldest + 1) % wb.len
		}
		wb.buf[index].ts = ts
		wb.buf[index].val = val
		wb.newest = index
	} else {
		wb.buf[index].ts = ts
		wb.buf[index].val = val
	}

	return true
}

// returns all the data in the buffer as a raw list of points
func (wb *WriteBuffer) Get() []schema.Point {
	res := make([]schema.Point, 0, wb.len)
	oldest := (wb.newest + 1) % wb.len

	for {
		if wb.buf[oldest].ts != 0 {
			res = append(res, schema.Point{Val: wb.buf[oldest].val, Ts: wb.buf[oldest].ts})
		}
		if oldest == wb.newest {
			break
		}
		oldest = (oldest + 1) % wb.len
	}

	return res
}
