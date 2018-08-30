package stats

import (
	"runtime"
	"time"
)

// MemoryReporter sources memory stats from the runtime and reports them
type MemoryReporter struct {
	mem           runtime.MemStats
	gcCyclesTotal uint32
}

func NewMemoryReporter() *MemoryReporter {
	return registry.getOrAdd("memory", &MemoryReporter{}).(*MemoryReporter)
}

func (m *MemoryReporter) ReportGraphite(prefix, buf []byte, now time.Time) []byte {
	runtime.ReadMemStats(&m.mem)

	// metric memory.total_bytes_allocated is a counter of total number of bytes allocated during process lifetime
	buf = WriteUint64(buf, prefix, []byte("total_bytes_allocated.counter64"), m.mem.TotalAlloc, now)

	// metric memory.bytes_allocated_on_heap is a gauge of currently allocated (within the runtime) memory.
	buf = WriteUint64(buf, prefix, []byte("bytes.allocated_in_heap.gauge64"), m.mem.Alloc, now)

	// metric memory.bytes.obtained_from_sys is the number of bytes currently obtained from the system by the process.  This is what the profiletrigger looks at.
	buf = WriteUint64(buf, prefix, []byte("bytes.obtained_from_sys.gauge64"), m.mem.Sys, now)

	// metric memory.total_gc_cycles is a counter of the number of GC cycles since process start
	buf = WriteUint32(buf, prefix, []byte("total_gc_cycles.counter64"), m.mem.NumGC, now)

	// metric memory.gc.cpu_fraction is how much cpu is consumed by the GC across process lifetime, in pro-mille
	buf = WriteUint32(buf, prefix, []byte("gc.cpu_fraction.gauge32"), uint32(1000*m.mem.GCCPUFraction), now)

	// metric memory.gc.heap_objects is how many objects are allocated on the heap, it's a key indicator for GC workload
	buf = WriteUint64(buf, prefix, []byte("gc.heap_objects.gauge64"), m.mem.HeapObjects, now)

	// there was no new GC run, we should only report points to represent actual runs
	if m.gcCyclesTotal != m.mem.NumGC {
		// metric memory.gc.last_duration is the duration of the last GC STW pause in nanoseconds
		buf = WriteUint64(buf, prefix, []byte("gc.last_duration.gauge64"), m.mem.PauseNs[(m.mem.NumGC+255)%256], now)
		m.gcCyclesTotal = m.mem.NumGC
	}

	return buf
}
