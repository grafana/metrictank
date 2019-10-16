package stats

import (
	"os"
	"runtime"
	"strconv"
	"time"

	"github.com/grafana/metrictank/util"
)

// MemoryReporter sources memory stats from the runtime and reports them
// It also reports gcPercent based on the GOGC environment variable
type MemoryReporter struct {
	mem                  runtime.MemStats
	gcCyclesTotal        uint32
	timeBoundGetMemStats func() interface{}
}

func NewMemoryReporter() *MemoryReporter {
	reporter := registry.getOrAdd("memory", &MemoryReporter{}).(*MemoryReporter)
	reporter.timeBoundGetMemStats = util.TimeBoundWithCacheFunc(func() interface{} {
		mem := runtime.MemStats{}
		runtime.ReadMemStats(&mem)
		return mem
	}, 5*time.Second, 1*time.Minute)
	return reporter
}

func getGcPercent() int {
	// follow standard runtime:
	// unparseable or not set -> 100
	// "off" -> -1
	gogc := os.Getenv("GOGC")
	if gogc == "" {
		return 100
	}
	if gogc == "off" {
		return -1
	}
	val, err := strconv.Atoi(gogc)
	if err != nil {
		return 100
	}
	return val
}

func (m *MemoryReporter) ReportGraphite(prefix, buf []byte, now time.Time) []byte {
	m.mem = m.timeBoundGetMemStats().(runtime.MemStats)
	gcPercent := getGcPercent()

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

	// metric memory.gc.gogc is the current GOGC value (derived from the GOGC environment variable)
	buf = WriteInt32(buf, prefix, []byte("gc.gogc.sgauge32"), int32(gcPercent), now)

	return buf
}
