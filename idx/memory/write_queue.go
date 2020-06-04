package memory

import (
	"fmt"
	"sync"
	"time"

	"github.com/grafana/metrictank/idx"
	"github.com/grafana/metrictank/schema"
)

type WriteQueue struct {
	shutdown     chan struct{}
	done         chan struct{}
	maxBuffered  int
	maxDelay     time.Duration
	flushTrigger chan struct{}

	archives map[schema.MKey]*idx.Archive
	sync.RWMutex

	idx *UnpartitionedMemoryIdx
}

// NewWriteQueue creates a new writeQueue that will add archives to the passed UnpartitionedMemoryIdx
// in batches
func NewWriteQueue(index *UnpartitionedMemoryIdx, maxDelay time.Duration, maxBuffered int) *WriteQueue {
	wq := &WriteQueue{
		archives:     make(map[schema.MKey]*idx.Archive),
		shutdown:     make(chan struct{}),
		done:         make(chan struct{}),
		maxBuffered:  maxBuffered,
		maxDelay:     maxDelay,
		flushTrigger: make(chan struct{}, 1),
		idx:          index,
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
		wq.flushTrigger <- struct{}{}
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
func (wq *WriteQueue) flush() {
	// Quick check to see if we have any archives we need to flush
	wq.Lock()
	archiveSize := len(wq.archives)
	wq.Unlock()

	if archiveSize == 0 {
		return
	}

	// wq.idx.Lock() can be very slow to acquire (if there are long read ops). wq.Lock has much
	// smaller bounds on lock hold time. So, to avoid blocking writes while waiting on the idx,
	// we make sure to acquire the index lock first and only then acquire wq.Lock

	bc := wq.idx.Lock()
	defer bc.Unlock("WriteQueueFlush", func() interface{} {
		return fmt.Sprintf("numAdds = %d", archiveSize)
	})

	wq.Lock()
	defer wq.Unlock()

	archiveSize = len(wq.archives)
	for _, archive := range wq.archives {
		wq.idx.add(archive)
	}
	wq.archives = make(map[schema.MKey]*idx.Archive)
<<<<<<< HEAD

	select {
	case wq.flushed <- struct{}{}:
	default:
	}
=======
>>>>>>> Use Priority lock, write queue flush is only called from flush loop
}

func (wq *WriteQueue) loop() {
	defer close(wq.done)
	timer := time.NewTimer(wq.maxDelay)
	for {
		select {
		case <-wq.flushTrigger:
			if !timer.Stop() {
				<-timer.C
			}
			wq.flush()
			timer.Reset(wq.maxDelay)
		case <-timer.C:
			wq.flush()
			timer.Reset(wq.maxDelay)
		case <-wq.shutdown:
			wq.flush()
			if !timer.Stop() {
				<-timer.C
			}
			return
		}
	}
}
