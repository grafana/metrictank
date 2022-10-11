package stats

import (
	"os"
	"time"

	"github.com/prometheus/procfs"
)

// ProcessReporter sources stats from /proc
type ProcessReporter struct {
	proc procfs.Proc
}

func NewProcessReporter() (*ProcessReporter, error) {
	p := ProcessReporter{}
	pid := os.Getpid()
	var err error
	p.proc, err = procfs.NewProc(pid)
	if err != nil {
		return nil, err
	}
	return registry.getOrAdd("process", &p).(*ProcessReporter), nil
}

func (m *ProcessReporter) WriteGraphiteLine(buf, prefix []byte, now time.Time) []byte {
	stat, err := m.proc.NewStat()

	if err == nil {
		vsz := uint64(stat.VirtualMemory())
		rss := uint64(stat.ResidentMemory())

		// metric process.virtual_memory_bytes.gauge64 is a gauge of the process VSZ from /proc/pid/stat
		buf = WriteUint64(buf, prefix, []byte("process.virtual_memory_bytes.gauge64"), nil, nil, vsz, now)

		// metric process.resident_memory_bytes.gauge64 is a gauge of the process RSS from /proc/pid/stat
		buf = WriteUint64(buf, prefix, []byte("process.resident_memory_bytes.gauge64"), nil, nil, rss, now)
		// metric process.minor_page_faults.counter64 is the number of minor faults the process has made which have not required loading a memory page from disk
		buf = WriteUint64(buf, prefix, []byte("process.minor_page_faults.counter64"), nil, nil, uint64(stat.MinFlt), now)

		// metric process.major_page_faults.counter64 is the number of major faults the process has made which have required loading a memory page from disk
		buf = WriteUint64(buf, prefix, []byte("process.major_page_faults.counter64"), nil, nil, uint64(stat.MajFlt), now)

		// metric is Total user and system CPU time spent in seconds
		buf = WriteFloat64(buf, prefix, []byte("process.cpu_seconds_total.counter64"), nil, nil, stat.CPUTime(), now)
	}

	return buf
}
