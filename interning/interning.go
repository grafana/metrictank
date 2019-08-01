package interning

import (
	"sync"

	"github.com/grafana/metrictank/stats"
	goi "github.com/robert-milan/go-object-interning"
)

// IdxIntern is a pointer into the object interning layer for the index
//
// Default config does not use compression
var IdxIntern = goi.NewObjectIntern(goi.NewConfig())

// metric recovered_errors.idx.memory.intern-error is how many times
// an error is encountered while attempting to intern a string.
// each time this happens, an error is logged with more details.
var internError = stats.NewCounter32("recovered_errors.idx.memory.intern-error")

// metric recovered_errors.idx.memory.invalid-tag is how many times
// an invalid tag for a metric is encountered.
// each time this happens, an error is logged with more details.
var invalidTag = stats.NewCounter32("recovered_errors.idx.memory.invalid-tag")

// MDIQueueItem stores a pointer to an interned
// MetricDefinition and a bool to determine if
// its reference counter needs to be incremented
// or decrememnted
type MDIQueueItem struct {
	*MetricDefinitionInterned
	Add bool
}

type InterningQueue struct {
	Queue    chan MDIQueueItem
	Finished chan struct{}
	sync.WaitGroup
}

func (iq *InterningQueue) Init() {
	iq.Queue = make(chan MDIQueueItem, 75000)
	iq.Finished = make(chan struct{})

	f := func() {
		var item MDIQueueItem
		var ok bool
		for {
			select {
			case item, ok = <-iq.Queue:
				if !ok {
					iq.Done()
					return
				}

				if item.Add {
					IdxIntern.IncRefCntBatchUnsafe(item.Name.Nodes())
					for i := range item.Tags.KeyValues {
						IdxIntern.IncRefCntBatchUnsafe([]uintptr{item.Tags.KeyValues[i].Key, item.Tags.KeyValues[i].Value})
					}

					if item.Unit != 0 {
						IdxIntern.IncRefCntUnsafe(uintptr(item.Unit))
					}
				} else {
					item.ReleaseInterned()
				}

			case <-iq.Finished:
				iq.Done()
				return
			}
		}
	}

	iq.Add(1)
	go f()
}

func (iq *InterningQueue) Stop() {
	iq.Finished <- struct{}{}
	iq.Wait()
	iq.Queue = nil
	iq.Finished = nil
}

var IdxInternQueue *InterningQueue
