package mdata

import (
	"fmt"
	"sync"

	"github.com/raintank/metrictank/conf"
	"gopkg.in/raintank/schema.v1"
)

var bufPool = sync.Pool{New: func() interface{} { return &entry{} }}

/*
 * The write buffer keeps a window of data during which it is ok to send data out of order.
 * Once the reorder window plus flush minimum has passed it will try to flush the data out.
 * The write buffer itself is not thread safe because it is only used by AggMetric,
 * which is thread safe, so there is no locking in the buffer.
 */

type WriteBuffer struct {
	reorderWindow uint32                // window size in datapoints during which out of order is allowed
	len           uint32                // number of datapoints in the buffer
	lastFlush     uint32                // the timestamp of the last point that's been flushed
	flushMin      uint32                // min number of datapoints to flush at once
	first         *entry                // first buffer entry
	last          *entry                // last buffer entry
	flush         func(uint32, float64) // flushing callback
}

type entry struct {
	ts         uint32
	val        float64
	next, prev *entry
}

func NewWriteBuffer(conf *conf.WriteBufferConf, flush func(uint32, float64)) *WriteBuffer {
	return &WriteBuffer{
		reorderWindow: conf.ReorderWindow,
		flushMin:      conf.FlushMin,
		flush:         flush,
	}
}

func (wb *WriteBuffer) Add(ts uint32, val float64) bool {
	// out of order and too old
	if ts < wb.lastFlush {
		return false
	}

	e := bufPool.Get().(*entry)
	e.ts = ts
	e.val = val

	// initializing the linked list
	if wb.first == nil {
		e.next = nil
		e.prev = nil
		wb.first = e
		wb.last = e
		wb.len++
	} else {
		if ts < wb.last.ts {
			metricsReordered.Inc()
		}

		// in the normal case data should be added in order, so this will only iterate once
		for i := wb.last; i != nil; i = i.prev {
			if ts > i.ts {
				if i.next == nil {
					wb.last = e
				} else {
					i.next.prev = e
				}
				e.next = i.next
				e.prev = i
				i.next = e
				e = nil
				wb.len++
				break
			}
			// overwrite value
			if ts == i.ts {
				i.val = val
				e = nil
				break
			}
		}
		if e != nil {
			// unlikely case where the added entry is the oldest one present
			e.prev = nil
			e.next = wb.first
			wb.first.prev = e
			wb.first = e
			wb.len++
		}
	}

	wb.flushIfReady()

	return true
}

// if buffer is ready for flushing, this will flush it
func (wb *WriteBuffer) flushIfReady() {
	// not enough data, not ready to flush
	if wb.len < wb.flushMin+wb.reorderWindow {
		return
	}

	// we want to flush until the length is equal to the reorder window
	flushCount := wb.len - wb.reorderWindow

	nextEntry := wb.first
	for i := uint32(0); i < flushCount; i++ {
		flushEntry := nextEntry
		nextEntry = flushEntry.next
		wb.flush(flushEntry.ts, flushEntry.val)
		bufPool.Put(flushEntry)
	}

	wb.len = wb.reorderWindow
	wb.first = nextEntry
	wb.first.prev = nil
	wb.lastFlush = wb.first.ts
}

// returns a formatted string that shows the current buffer content,
// only used for debugging purposes and should never be called in prod
func (wb *WriteBuffer) formatted() string {
	var str string
	var id int
	str = fmt.Sprintf("Buffer len: %d first: %p last: %p \n", wb.len, wb.first, wb.last)
	for i := wb.first; i != nil; i = i.next {
		str = fmt.Sprintf(
			"%sId: %d ts: %d val: %f addr: %p prev: %p next: %p\n",
			str, id, i.ts, i.val, i, i.prev, i.next,
		)
		id++
	}
	return str
}

// returns all the data in the buffer as a raw list of points
func (wb *WriteBuffer) Get() []schema.Point {
	res := make([]schema.Point, 0, wb.len)
	if wb.first == nil {
		return res
	}

	for i := wb.first; i != nil; i = i.next {
		res = append(res, schema.Point{Val: i.val, Ts: i.ts})
	}

	return res
}
