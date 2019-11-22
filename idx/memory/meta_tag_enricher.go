package memory

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/grafana/metrictank/expr/tagquery"
	"github.com/grafana/metrictank/idx"
	"github.com/grafana/metrictank/schema"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

// metaTagEnricher is responsible for "enriching" metrics resulting from a query by
// looking up their associations with the defined meta records.
// there are 4 operations to modify it's state, they all get executed asynchronously
// via a queue which gets consumed by a worker thread. that's why the enricher also has
// its own locking which is independent from the main index lock.
// following are the 4 state changing operations:
// 1) adding a meta record
// 2) deleting a meta record
// 3) adding a metric key
// 4) deleting a metric key
// all used metric keys are org-agnostic because we instantiate one enricher per org
type metaTagEnricher struct {
	// the enricher lock
	sync.RWMutex
	// wait group to wait for the worker on shutdown
	wg sync.WaitGroup
	// mapping from metric key to list of meta record ids, used to do the enrichment
	recordsByMetric map[schema.Key]map[recordId]struct{}
	// mapping from record ids to tag queries associated with given record
	queriesByRecord map[recordId]tagquery.Query
	// the event queue, all state changing operations get pushed through this queue
	eventQueue chan enricherEvent
	// buffer that's used to buffer addMetric events, because executing them in bunches
	// is more efficient (faster) than executing them one-by-one
	addMetricBuffer []schema.MetricDefinition
	// pool of idx.Archive structs that we use when processing add metric events
	archivePool sync.Pool

	idLookup idLookup
}

type enricherEventType uint8

const (
	addMetric enricherEventType = iota
	delMetric
	addMetaRecord
	delMetaRecord
	shutdown
)

type enricherEvent struct {
	// eventType gets looked up from the eventHandlers map to obtain the handler for the payload
	eventType enricherEventType
	payload   interface{}
}

func newEnricher(idLookup idLookup) *metaTagEnricher {
	res := &metaTagEnricher{
		queriesByRecord: make(map[recordId]tagquery.Query),
		recordsByMetric: make(map[schema.Key]map[recordId]struct{}),
		archivePool: sync.Pool{
			New: func() interface{} { return &idx.Archive{} },
		},
		idLookup: idLookup,
	}

	res.start()
	return res
}

// start creates the go routine which consumes the event queue and processes received events
func (e *metaTagEnricher) start() {
	e.eventQueue = make(chan enricherEvent, metaTagEnricherQueueSize)

	e.wg.Add(1)
	go func() {
		defer e.wg.Done()

		var event enricherEvent
		timer := time.NewTimer(metaTagEnricherBufferTime)

		for {
			select {
			case <-timer.C:
				timer.Reset(metaTagEnricherBufferTime)
				e._flushAddMetricBuffer()
			case event = <-e.eventQueue:
				if event.eventType != addMetric && len(e.addMetricBuffer) > 0 {
					e._flushAddMetricBuffer()
				}

				switch event.eventType {
				case addMetric:
					e._bufferAddMetric(event.payload)

					if len(e.addMetricBuffer) >= metaTagEnricherBufferSize {
						e._flushAddMetricBuffer()
					}

					// reset timer to only trigger after add metric buffer timeout because
					// if we're in a situation where new metrics keep getting added we want
					// to first fill the buffer before processing the whole bunch
					if !timer.Stop() {
						<-timer.C
					}
					timer.Reset(metaTagEnricherBufferTime)
				case delMetric:
					e._delMetric(event.payload)
				case addMetaRecord:
					e._addMetaRecord(event.payload)
				case delMetaRecord:
					e._delMetaRecord(event.payload)
				case shutdown:
					return
				}
			}
		}
	}()
}

func (e *metaTagEnricher) _flushAddMetricBuffer() {
	if len(e.addMetricBuffer) == 0 {
		// buffer is empty, nothing to do
		return
	}

	tags := make(TagIndex)
	defById := make(map[schema.MKey]*idx.Archive, len(e.addMetricBuffer))

	defer func() {
		for _, archive := range defById {
			e.archivePool.Put(archive)
		}
		e.addMetricBuffer = e.addMetricBuffer[:0]
	}()

	// build a small tag index with all the metrics in the buffer
	// we later use that index to run the meta record queries on it
	for _, md := range e.addMetricBuffer {
		for _, tag := range md.Tags {
			tagSplits := strings.SplitN(tag, "=", 2)
			if len(tagSplits) < 2 {
				// should never happen because every tag in the index
				// must have a valid format
				invalidTag.Inc()
				log.Errorf("memory-idx: Tag %q of id %q has an invalid format", tag, md.Id)
				continue
			}
			tags.addTagId(tagSplits[0], tagSplits[1], md.Id)
		}
		tags.addTagId("name", md.NameSanitizedAsTagValue(), md.Id)

		archive := e.archivePool.Get().(*idx.Archive)
		archive.MetricDefinition = md
		defById[md.Id] = archive
	}

	recordCh := make(chan recordId, len(e.queriesByRecord))
	resultCh := make(chan struct {
		record recordId
		keys   []schema.Key
	}, len(e.queriesByRecord))
	group, _ := errgroup.WithContext(context.Background())

	// execute all the meta record queries on the small index we built above
	// and collect their results in the resultCh
	// the queries run concurrently in a group of workers
	for i := 0; i < TagQueryWorkers; i++ {
		group.Go(func() error {
			var queryCtx TagQueryContext
			for record := range recordCh {
				result := struct {
					record recordId
					keys   []schema.Key
				}{record: record}
				queryCtx = NewTagQueryContext(e.queriesByRecord[record])
				idCh := make(chan schema.MKey, 100)
				queryCtx.RunNonBlocking(tags, defById, nil, nil, idCh)
				for id := range idCh {
					result.keys = append(result.keys, id.Key)
				}
				if len(result.keys) > 0 {
					resultCh <- result
				}
			}
			return nil
		})
	}

	go func() {
		group.Wait()
		close(resultCh)
	}()

	// feed all records ids for which we have a query into recordCh
	// to make the query workers execute them
	for record := range e.queriesByRecord {
		recordCh <- record
	}
	close(recordCh)

	// collect all results before applying them to the enricher to
	// keep the time during which we need to hold the write lock as
	// short as possible
	results := make(map[recordId][]schema.Key)
	for result := range resultCh {
		results[result.record] = result.keys
	}

	e.Lock()
	defer e.Unlock()

	for record, keys := range results {
		for _, key := range keys {
			if _, ok := e.recordsByMetric[key]; !ok {
				e.recordsByMetric[key] = make(map[recordId]struct{})
			}
			e.recordsByMetric[key][record] = struct{}{}
		}
	}

	enricherMetricsWithMetaRecords.SetUint32(uint32(len(e.recordsByMetric)))
}

func (e *metaTagEnricher) _bufferAddMetric(payload interface{}) {
	e.addMetricBuffer = append(e.addMetricBuffer, payload.(schema.MetricDefinition))
}

func (e *metaTagEnricher) stop() {
	e.eventQueue <- enricherEvent{eventType: shutdown}
	e.wg.Wait()
}

func (e *metaTagEnricher) addMetric(md schema.MetricDefinition) {
	e.eventQueue <- enricherEvent{
		eventType: addMetric,
		payload:   md,
	}
}

func (e *metaTagEnricher) delMetric(md *schema.MetricDefinition) {
	e.eventQueue <- enricherEvent{
		eventType: delMetric,
		payload:   md,
	}
}

func (e *metaTagEnricher) _delMetric(payload interface{}) {
	md := payload.(*schema.MetricDefinition)
	e.Lock()
	delete(e.recordsByMetric, md.Id.Key)
	e.Unlock()
	enricherMetricsWithMetaRecords.SetUint32(uint32(len(e.recordsByMetric)))
}

func (e *metaTagEnricher) addMetaRecord(id recordId, query tagquery.Query) {
	handleResultCh := func(idCh chan schema.MKey) {
		e.eventQueue <- enricherEvent{
			eventType: addMetaRecord,
			payload: struct {
				recordId recordId
				query    tagquery.Query
				idCh     chan schema.MKey
			}{recordId: id, query: query, idCh: idCh},
		}
	}

	e.idLookup(query, handleResultCh)
}

func (e *metaTagEnricher) _addMetaRecord(payload interface{}) {
	data := payload.(struct {
		recordId recordId
		query    tagquery.Query
		idCh     chan schema.MKey
	})

	var added uint32

	e.Lock()
	for id := range data.idCh {
		if _, ok := e.recordsByMetric[id.Key]; ok {
			e.recordsByMetric[id.Key][data.recordId] = struct{}{}
		} else {
			e.recordsByMetric[id.Key] = map[recordId]struct{}{
				data.recordId: {},
			}
		}
		added++
	}
	e.queriesByRecord[data.recordId] = data.query
	e.Unlock()

	enricherMetricsWithMetaRecords.SetUint32(uint32(len(e.recordsByMetric)))
	enricherMetricsAddedByQuery.AddUint32(added)
}

func (e *metaTagEnricher) delMetaRecord(id recordId) {
	e.RLock()
	query := e.queriesByRecord[id]
	e.RUnlock()

	handleResultCh := func(idCh chan schema.MKey) {
		e.eventQueue <- enricherEvent{
			eventType: delMetaRecord,
			payload: struct {
				recordId recordId
				idCh     chan schema.MKey
			}{recordId: id, idCh: idCh},
		}
	}

	e.idLookup(query, handleResultCh)
}

func (e *metaTagEnricher) _delMetaRecord(payload interface{}) {
	data := payload.(struct {
		recordId recordId
		idCh     chan schema.MKey
	})

	e.Lock()
	for id := range data.idCh {
		delete(e.recordsByMetric[id.Key], data.recordId)
		if len(e.recordsByMetric[id.Key]) == 0 {
			delete(e.recordsByMetric, id.Key)
		}
	}
	e.Unlock()

	e.Lock()
	delete(e.queriesByRecord, data.recordId)
	enricherKnownMetaRecords.SetUint32(uint32(len(e.queriesByRecord)))
	e.Unlock()
}

// enrich resolves a metric key into the associated set of record ids,
// it is the most performance critical method of the enricher because
// it is used inside the query processing
func (e *metaTagEnricher) enrich(key schema.Key) map[recordId]struct{} {
	e.RLock()
	defer e.RUnlock()
	return e.recordsByMetric[key]
}

// countMetricWithMetaTags returns the total number of known metrics which
// have one or more meta tags associated
func (e *metaTagEnricher) countMetricsWithMetaTags() int {
	e.RLock()
	defer e.RUnlock()
	return len(e.recordsByMetric)
}
