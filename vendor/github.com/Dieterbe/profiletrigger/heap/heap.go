package heap

import (
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"
	"time"
)

// Heap will check every checkEvery for memory obtained from the system, by the process
// - using the metric Sys at https://golang.org/pkg/runtime/#MemStats -
// whether it reached or exceeds the threshold and take a memory profile to the path directory,
// but no more often than every minTimeDiff seconds
// any errors will be sent to the errors channel
type Heap struct {
	path        string
	threshold   int
	minTimeDiff int
	checkEvery  time.Duration
	lastUnix    int64
	Errors      chan error
}

// New creates a new Heap trigger. use a nil channel if you don't care about any errors
func New(path string, threshold, minTimeDiff int, checkEvery time.Duration, errors chan error) (*Heap, error) {
	heap := Heap{
		path,
		threshold,
		minTimeDiff,
		checkEvery,
		int64(0),
		errors,
	}
	return &heap, nil
}

func (heap Heap) logError(err error) {
	if heap.Errors != nil {
		heap.Errors <- err
	}
}

// Run runs the trigger. encountered errors go to the configured channel (if any).
// you probably want to run this in a new goroutine.
func (heap Heap) Run() {
	tick := time.NewTicker(heap.checkEvery)
	m := &runtime.MemStats{}
	for ts := range tick.C {
		runtime.ReadMemStats(m)
		unix := ts.Unix()
		if m.Sys >= uint64(heap.threshold) && unix >= heap.lastUnix+int64(heap.minTimeDiff) {
			f, err := os.Create(fmt.Sprintf("%s/%d.profile-heap", heap.path, unix))
			if err != nil {
				heap.logError(err)
				continue
			}
			err = pprof.WriteHeapProfile(f)
			if err != nil {
				heap.logError(err)
			}
			heap.lastUnix = unix
			err = f.Close()
			if err != nil {
				heap.logError(err)
			}
		}
	}
}
