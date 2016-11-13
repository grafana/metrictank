package stats

import (
	"runtime"
)

// MemoryReporter sources memory stats from the runtime and reports them
type MemoryReporter struct {
	mem           runtime.MemStats
	gcCyclesTotal uint32
}

func NewMemoryReporter() *MemoryReporter {
	return registry.add("memory", func() GraphiteMetric {
		return &MemoryReporter{}
	}).(*MemoryReporter)
}

func (m *MemoryReporter) ReportGraphite(prefix, buf []byte, now int64) []byte {
	runtime.ReadMemStats(&m.mem)

	// metric total_bytes_allocated is a counter of total amount of bytes allocated during process lifetime
	buf = WriteUint64(buf, prefix, []byte("total_bytes_allocated"), m.mem.TotalAlloc, now)

	// metric bytes_allocated_on_heap  is a gauge of currently allocated (within the runtime) memory.
	buf = WriteUint64(buf, prefix, []byte("bytes.allocated_in_heap"), m.mem.Alloc, now)

	// metric bytes.obtained_from_sys is the amount of bytes currently obtained from the system by the process.  This is what the profiletrigger looks at.
	buf = WriteUint64(buf, prefix, []byte("bytes.obtained_from_sys"), m.mem.Sys, now)

	// metric total_gc_cycles is a counter of the number of GC cycles since process start
	buf = WriteUint32(buf, prefix, []byte("total_gc_cycles"), m.mem.NumGC, now)

	// metric gc.cpu_fraction is how much cpu is consumed by the GC, in pro-mille
	buf = WriteUint32(buf, prefix, []byte("gc.cpu_fraction"), uint32(1000*m.mem.GCCPUFraction), now)

	// metric gc.heap_objects is how many objects are allocated on the heap, it's a key indicator for GC workload
	buf = WriteUint64(buf, prefix, []byte("gc.heap_objects"), m.mem.HeapObjects, now)

	// there was no new GC run, we should only report points to represent actual runs
	if m.gcCyclesTotal != m.mem.NumGC {
		// metric gc.last_duration is the duration of the last GC STW pause in nanoseconds
		buf = WriteUint64(buf, prefix, []byte("gc.last_duration"), m.mem.PauseNs[(m.mem.NumGC+255)%256], now)
		m.gcCyclesTotal = m.mem.NumGC
	}

	return buf
}
