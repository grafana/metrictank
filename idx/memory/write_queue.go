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
	flushPending bool

	archives map[uint32]map[schema.MKey]*idx.Archive
	sync.RWMutex

	idx *UnpartitionedMemoryIdx
}

// NewWriteQueue creates a new writeQueue that will add archives to the passed UnpartitionedMemoryIdx
// in batches
func NewWriteQueue(index *UnpartitionedMemoryIdx, maxDelay time.Duration, maxBuffered int) *WriteQueue {
	wq := &WriteQueue{
		archives:     make(map[uint32]map[schema.MKey]*idx.Archive),
		shutdown:     make(chan struct{}),
		done:         make(chan struct{}),
		maxBuffered:  maxBuffered,
		maxDelay:     maxDelay,
		flushTrigger: make(chan struct{}, 1),
		flushPending: false,
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
	if _, ok := wq.archives[archive.Id.Org]; !ok {
		wq.archives[archive.Id.Org] = make(map[schema.MKey]*idx.Archive)
	}
	wq.archives[archive.OrgId][archive.Id] = archive
	if !wq.flushPending && len(wq.archives) >= wq.maxBuffered {
		wq.flushPending = true
		select {
		case wq.flushTrigger <- struct{}{}:
		default:
		}
	}
	wq.Unlock()
}

func (wq *WriteQueue) Get(id schema.MKey) (*idx.Archive, bool) {
	wq.RLock()
	if _, ok := wq.archives[id.Org]; !ok {
		// doesn't exist, nothing to get
		wq.RUnlock()
		return &idx.Archive{}, false
	}
	a, ok := wq.archives[id.Org][id]
	wq.RUnlock()
	return a, ok
}

// flush adds the buffered archives to the memoryIdx.
func (wq *WriteQueue) flush() {
	var archiveSize int
	// Quick check to see if we have any archives we need to flush
	wq.Lock()
	for _, archives := range wq.archives {
		archiveSize += len(archives)
		wq.flushPending = archiveSize > 0
	}
	wq.Unlock()

	if archiveSize == 0 {
		return
	}

	pre := time.Now()

	wq.Lock()
	defer wq.Unlock()

	for orgID, archives := range wq.archives {
		archiveSize = len(archives)
		bc := wq.idx.orgLocks[orgID].Lock()
		for _, archive := range archives {
			wq.idx.add(archive)
		}
		bc.Unlock(fmt.Sprintf("WriteQueueFlush - Org %d", orgID), func() interface{} {
			return fmt.Sprintf("numAdds = %d", archiveSize)
		})
	}
	wq.archives = make(map[uint32]map[schema.MKey]*idx.Archive)
	wq.flushPending = false

	statAddDuration.Value(time.Since(pre))
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
