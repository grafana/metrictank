package memory

import (
	"sync"
	"time"

	"github.com/grafana/metrictank/idx"
	"github.com/grafana/metrictank/schema"
)

type WriteQueue struct {
	shutdown    chan struct{}
	done        chan struct{}
	maxBuffered int
	maxDelay    time.Duration
	flushed     chan struct{}

	archives map[schema.MKey]*idx.Archive
	sync.RWMutex

	idx *UnpartitionedMemoryIdx
}

// NewWriteQueue creates a new writeQueue that will add archives to the passed UnpartitionedMemoryIdx
// in batches
func NewWriteQueue(index *UnpartitionedMemoryIdx, maxDelay time.Duration, maxBuffered int) *WriteQueue {
	wq := &WriteQueue{
		archives:    make(map[schema.MKey]*idx.Archive),
		shutdown:    make(chan struct{}),
		done:        make(chan struct{}),
		maxBuffered: maxBuffered,
		maxDelay:    maxDelay,
		flushed:     make(chan struct{}, 1),
		idx:         index,
	}
	go wq.loop()
	return wq
}

func (wq *WriteQueue) Stop() {
	close(wq.shutdown)
	<-wq.done
}

func (wq *WriteQueue) Queue(archive *idx.Archive) {
	wq.Lock()
	wq.archives[archive.Id] = archive
	if len(wq.archives) >= wq.maxBuffered {
		wq.flush()
	}
	wq.Unlock()
}

func (wq *WriteQueue) Get(id schema.MKey) (*idx.Archive, bool) {
	wq.RLock()
	a, ok := wq.archives[id]
	wq.RUnlock()
	return a, ok
}

// flush adds the buffered archives to the memoryIdx.
// callers need to acquire a writeLock before calling this function.
func (wq *WriteQueue) flush() {
	if len(wq.archives) == 0 {
		// non blocking write to the flushed chan.
		// if we cant write to the flushed chan it means there is a previous flush
		// signal that hasnt been processed.  In that case, we dont need to send another one.
		select {
		case wq.flushed <- struct{}{}:
		default:
		}
		return
	}
	for _, archive := range wq.archives {
		wq.idx.add(archive)
	}
	wq.archives = make(map[schema.MKey]*idx.Archive)
	wq.flushed <- struct{}{}
}

func (wq *WriteQueue) loop() {
	defer close(wq.done)
	timer := time.NewTimer(wq.maxDelay)
	for {
		select {
		case <-wq.flushed:
			if !timer.Stop() {
				<-timer.C
			}
			timer.Reset(wq.maxDelay)
		case <-timer.C:
			wq.Lock()
			wq.flush()
			wq.Unlock()
			timer.Reset(wq.maxDelay)
		case <-wq.shutdown:
			wq.Lock()
			wq.flush()
			wq.Unlock()
			if !timer.Stop() {
				<-timer.C
			}
			return
		}
	}
}
