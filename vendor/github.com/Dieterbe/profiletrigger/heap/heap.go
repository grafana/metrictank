package heap

import (
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"
	"time"

	"github.com/Dieterbe/profiletrigger/procfs"
)

// Heap will check memory at the requested interval and report profiles if thresholds are breached.
// See Config for configuration documentation
// If non-nil, any runtime errors are sent on the errors channel
type Heap struct {
	cfg           Config
	errors        chan error
	lastTriggered time.Time
	proc          procfs.Proc
}

// Config is the config for triggering heap profiles
// It uses HeapAlloc from https://golang.org/pkg/runtime/#MemStats as well as RSS memory usage
type Config struct {
	Path        string        // directory to write profiles to.
	ThreshHeap  int           // number of bytes to compare MemStats.HeapAlloc (bytes of allocated heap objects) to
	ThreshRSS   int           // number of bytes to compare RSS usage to
	MinTimeDiff time.Duration // report no more often than this
	CheckEvery  time.Duration // check both thresholds at this rate
}

// New creates a new Heap trigger. use a nil channel if you don't care about any errors
func New(cfg Config, errors chan error) (*Heap, error) {
	heap := Heap{
		cfg:    cfg,
		errors: errors,
	}
	if cfg.ThreshRSS != 0 {
		proc, err := procfs.Self()
		if err != nil {
			return nil, err
		}
		heap.proc = proc
	}

	return &heap, nil
}

func (heap Heap) logError(err error) {
	if heap.errors != nil {
		heap.errors <- err
	}
}

// Run runs the trigger. any encountered errors go to the configured errors channel.
// you probably want to run this in a new goroutine.
func (heap Heap) Run() {
	cfg := heap.cfg
	tick := time.NewTicker(cfg.CheckEvery)

	for ts := range tick.C {
		if !heap.shouldProfile(ts) {
			continue
		}
		f, err := os.Create(fmt.Sprintf("%s/%d.profile-heap", cfg.Path, ts.Unix()))
		if err != nil {
			heap.logError(err)
			continue
		}
		err = pprof.WriteHeapProfile(f)
		if err != nil {
			heap.logError(err)
		}
		heap.lastTriggered = ts
		err = f.Close()
		if err != nil {
			heap.logError(err)
		}
	}
}

func (heap Heap) shouldProfile(ts time.Time) bool {
	cfg := heap.cfg

	if ts.Before(heap.lastTriggered.Add(cfg.MinTimeDiff)) {
		return false
	}

	// Check RSS.
	if cfg.ThreshRSS != 0 {
		stat, err := heap.proc.NewStat()
		if err != nil {
			heap.logError(err)
		} else if stat.ResidentMemory() >= cfg.ThreshRSS {
			return true
		}
	}

	// Check HeapAlloc
	if cfg.ThreshHeap != 0 {
		m := &runtime.MemStats{}
		runtime.ReadMemStats(m)

		if m.HeapAlloc >= uint64(cfg.ThreshHeap) {
			return true
		}
	}

	return false
}
