package memory

import (
	"context"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/grafana/metrictank/errors"
	"github.com/grafana/metrictank/expr/tagquery"
	"github.com/grafana/metrictank/idx"
	"github.com/grafana/metrictank/schema"
	"github.com/grafana/metrictank/stats"
	log "github.com/sirupsen/logrus"
)

var (
	// the collision avoidance window defines how many times we try to find a higher
	// slot that's free if two record hashes collide
	collisionAvoidanceWindow = uint32(1024)

	// metrics idx.memory.meta-tags.enricher.ops.metrics-added-by-query is a counter of metrics that get added to the enricher by executing tag queries
	enricherMetricsAddedByQuery = stats.NewCounter32("idx.memory.meta-tags.enricher.ops.metrics-added-by-query")

	// metric idx.memory.meta-tags.enricher.ops.known-meta-records is a counter of meta records known to the enricher
	enricherKnownMetaRecords = stats.NewCounter32("idx.memory.meta-tags.enricher.ops.known-meta-records")

	// metric idx.memory.meta-tags.enricher.ops.metrics-with-meta-records is a counter of metrics with at least one associated meta record
	enricherMetricsWithMetaRecords = stats.NewCounter32("idx.memory.meta-tags.enricher.ops.metrics-with-meta-records")

	// metric idx.memory.meta-tags.swap.ops.executing counts the number of swap calls, each call means that the backend store has detected a change in the meta records
	metaRecordSwapExecuting = stats.NewCounter32("idx.memory.meta-tags.swap.ops.executing")

	// metric idx.memory.meta-tags.swap.ops.meta-records-unchanged counts the number of meta records that have not changed at all
	metaRecordSwapUnchanged = stats.NewCounter32("idx.memory.meta-tags.swap.ops.meta-records-unchanged")

	// metric idx.memory.meta-tags.swap.ops.meta-records-added counts the number of meta records that have been added
	metaRecordSwapAdded = stats.NewCounter32("idx.memory.meta-tags.swap.ops.meta-records-added")

	// metric idx.memory.meta-tags.swap.ops.meta-records-modified counts the number of meta records that have already existed, but had different meta tags
	metaRecordSwapModified = stats.NewCounter32("idx.memory.meta-tags.swap.ops.meta-records-modified")

	// metric idx.memory.meta-tags.swap.ops.meta-records-pruned counts the number of meta records that have been pruned from the index because in the latest swap they were not present
	metaRecordSwapPruned = stats.NewCounter32("idx.memory.meta-tags.swap.ops.meta-records-pruned")
)

type recordId uint32

// list of meta records keyed by a unique identifier used as ID
type metaTagRecords struct {
	metaRecordLock sync.Mutex // used to ensure that we never run multiple swap operations concurrently
	records        map[recordId]tagquery.MetaTagRecord
}

func newMetaTagRecords() *metaTagRecords {
	return &metaTagRecords{
		records: make(map[recordId]tagquery.MetaTagRecord),
	}
}

func (m *metaTagRecords) length() int {
	return len(m.records)
}

func (m *metaTagRecords) prune(toPrune map[recordId]struct{}, pruned map[recordId]tagquery.MetaTagRecord) {
	for recordId := range toPrune {
		pruned[recordId] = m.records[recordId]
		delete(m.records, recordId)
	}
}

func (m *metaTagRecords) getPrunable(toKeep map[recordId]struct{}) map[recordId]struct{} {
	toPrune := make(map[recordId]struct{}, len(m.records)-len(toKeep))
	for recordId := range m.records {
		if _, ok := toKeep[recordId]; !ok {
			toPrune[recordId] = struct{}{}
		}
	}
	return toPrune
}

func (m *metaTagRecords) getMetaTagsByRecordIds(recordIds map[recordId]struct{}) tagquery.Tags {
	res := make(tagquery.Tags, 0, len(recordIds))
	for recordId := range recordIds {
		record, ok := m.records[recordId]
		if ok {
			res = append(res, record.MetaTags...)
		}
	}
	return res
}

// recordExists takes a meta record and checks if it exists
// the identity of a record is determined by its set of query expressions, so if there is
// any other record with the same query expressions this method returns the id, the record,
// and true. if it doesn't exist the third return value is false. it is assumed that the
// expressions of the given record are sorted.
func (m *metaTagRecords) recordExists(record tagquery.MetaTagRecord) (recordId, *tagquery.MetaTagRecord, bool) {
	id := recordId(record.HashExpressions())

	// loop over existing records, starting from id, trying to find one that has
	// the exact same queries as the one we're upserting
	for i := uint32(0); i < collisionAvoidanceWindow; i++ {
		checkingId := id + recordId(i)
		if existingRecord, ok := m.records[checkingId]; ok {
			if record.Expressions.Equal(existingRecord.Expressions) {
				return checkingId, &existingRecord, true
			}
		}
	}

	return 0, nil, false
}

// recordExistsAndIsEqual checks if the given record exists
// if it exists it also checks if the present record is equal to the given one (has the same meta tags).
// it is assumed that the expressions of the given record are sorted.
// - the first return value is the record id of the existing record, if there is one
// - the second return value indicates whether the given record exists
// - the third return value indicates whether the existing record is equal (has the same meta tags)
func (m *metaTagRecords) recordExistsAndIsEqual(record tagquery.MetaTagRecord) (recordId, bool, bool) {
	id, existingRecord, exists := m.recordExists(record)
	if !exists {
		return id, false, false
	}

	return id, true, record.MetaTags.Equal(existingRecord.MetaTags)
}

// upsert inserts or updates a meta tag record according to the given specifications
// it uses the set of tag query expressions as the identity of the record, if a record with the
// same identity is already present then its meta tags get updated to the specified ones.
// If the new record contains no meta tags, then the update is equivalent to a delete.
// Those are the return values:
// 1) The id at which the new record got inserted
// 2) Pointer to the inserted metaTagRecord
// 3) The id of the record that has been replaced if an update was performed
// 4) Pointer to the metaTagRecord that has been replaced if an update was performed, otherwise nil
// 5) Error if an error occurred, otherwise it's nil
func (m *metaTagRecords) upsert(record tagquery.MetaTagRecord) (recordId, *tagquery.MetaTagRecord, recordId, *tagquery.MetaTagRecord, error) {
	record.Expressions.Sort()

	oldId, oldRecord, exists := m.recordExists(record)
	if exists {
		delete(m.records, oldId)
	}

	if !record.HasMetaTags() {
		return 0, &record, oldId, oldRecord, nil
	}

	record.MetaTags.Sort()
	id := recordId(record.HashExpressions())

	// now find the best position to insert the new/updated record, starting from id
	for i := uint32(0); i < collisionAvoidanceWindow; i++ {
		// if we find a free slot, then insert the new record there
		if _, ok := m.records[id]; !ok {
			m.records[id] = record

			return id, &record, oldId, oldRecord, nil
		}

		id++
	}

	return 0, nil, 0, nil, errors.NewInternal("Could not find a free ID to insert record")
}

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

func newEnricher() *metaTagEnricher {
	res := &metaTagEnricher{
		queriesByRecord: make(map[recordId]tagquery.Query),
		recordsByMetric: make(map[schema.Key]map[recordId]struct{}),
		archivePool: sync.Pool{
			New: func() interface{} { return &idx.Archive{} },
		},
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

func (e *metaTagEnricher) addMetaRecord(id recordId, query tagquery.Query, keys []schema.Key) {
	e.eventQueue <- enricherEvent{
		eventType: addMetaRecord,
		payload: struct {
			id    recordId
			query tagquery.Query
			keys  []schema.Key
		}{id: id, query: query, keys: keys},
	}
}

func (e *metaTagEnricher) _addMetaRecord(payload interface{}) {
	data := payload.(struct {
		id    recordId
		query tagquery.Query
		keys  []schema.Key
	})

	e.Lock()
	e.queriesByRecord[data.id] = data.query
	e.Unlock()
	enricherKnownMetaRecords.SetUint32(uint32(len(e.queriesByRecord)))

	for _, key := range data.keys {
		// we acquire one write lock per iteration, instead of one for the whole loop,
		// because the performance of addMetaRecord operations is less important than
		// the enrich() calls which are used by the query processing
		e.Lock()
		if _, ok := e.recordsByMetric[key]; ok {
			e.recordsByMetric[key][data.id] = struct{}{}
			e.Unlock()
			continue
		}
		e.recordsByMetric[key] = map[recordId]struct{}{data.id: {}}
		e.Unlock()
	}

	enricherMetricsWithMetaRecords.SetUint32(uint32(len(e.recordsByMetric)))
	enricherMetricsAddedByQuery.Add(len(data.keys))
}

func (e *metaTagEnricher) delMetaRecord(id recordId, keys []schema.Key) {
	e.eventQueue <- enricherEvent{
		eventType: delMetaRecord,
		payload: struct {
			id   recordId
			keys []schema.Key
		}{id: id, keys: keys},
	}
}

func (e *metaTagEnricher) _delMetaRecord(payload interface{}) {
	data := payload.(struct {
		id   recordId
		keys []schema.Key
	})

	// before deleting the meta record from the e.queriesByRecord map we
	// delete all references to it from the recordsByMetric map
	for _, key := range data.keys {
		e.Lock()
		delete(e.recordsByMetric[key], data.id)
		if len(e.recordsByMetric[key]) == 0 {
			delete(e.recordsByMetric, key)
		}
		e.Unlock()
	}
	enricherMetricsWithMetaRecords.SetUint32(uint32(len(e.recordsByMetric)))

	e.Lock()
	delete(e.queriesByRecord, data.id)
	e.Unlock()
	enricherKnownMetaRecords.SetUint32(uint32(len(e.queriesByRecord)))
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

// index structure keyed by tag -> value -> list of meta record IDs
type metaTagValue map[string][]recordId
type metaTagHierarchy map[string]metaTagValue

func (m metaTagHierarchy) deleteRecord(keyValue tagquery.Tag, id recordId) {
	if ids, ok := m[keyValue.Key][keyValue.Value]; ok {
		for i := 0; i < len(ids); i++ {
			if ids[i] == id {
				// no need to keep the order
				ids[i] = ids[len(ids)-1]
				m[keyValue.Key][keyValue.Value] = ids[:len(ids)-1]
				break
			}
		}
		if len(m[keyValue.Key][keyValue.Value]) == 0 {
			delete(m[keyValue.Key], keyValue.Value)
			if len(m[keyValue.Key]) == 0 {
				delete(m, keyValue.Key)
			}
		}
	}
}

func (m metaTagHierarchy) insertRecord(keyValue tagquery.Tag, id recordId) {
	var values metaTagValue
	var ok bool

	if values, ok = m[keyValue.Key]; !ok {
		values = make(metaTagValue)
		m[keyValue.Key] = values
	}

	values[keyValue.Value] = append(values[keyValue.Value], id)
}

// getMetaRecordIdsByExpression takes an expression and a bool, it returns all meta record
// ids of the records which match it. the bool indicates whether the result set should be
// inverted. In some cases inverting the result set can minimize its size.
// Minimizing the size of the returned result set will lead to a faster expression evaluation,
// because less meta records will need to be checked against a given MetricDefinition.
// The caller, after receiving the result set, needs to be aware of whether the result set
// is inverted and interpret it accordingly.
func (m metaTagHierarchy) getMetaRecordIdsByExpression(expr tagquery.Expression, invertSetOfMetaRecords bool) []recordId {
	if expr.OperatesOnTag() {
		return m.getByTag(expr, invertSetOfMetaRecords)
	}
	return m.getByTagValue(expr, invertSetOfMetaRecords)
}

func (m metaTagHierarchy) getByTag(expr tagquery.Expression, invertSetOfMetaRecords bool) []recordId {
	var res []recordId

	for key := range m {

		if invertSetOfMetaRecords {
			if expr.Matches(key) {
				continue
			}
		} else {
			if !expr.Matches(key) {
				continue
			}
		}

		for _, ids := range m[key] {
			res = append(res, ids...)
		}
	}

	return res
}

func (m metaTagHierarchy) getByTagValue(expr tagquery.Expression, invertSetOfMetaRecords bool) []recordId {
	if expr.MatchesExactly() {
		return m[expr.GetKey()][expr.GetValue()]
	}

	var res []recordId
	for value, ids := range m[expr.GetKey()] {
		passes := expr.Matches(value)

		if invertSetOfMetaRecords {
			passes = !passes
		}

		if !passes {
			continue
		}

		res = append(res, ids...)
	}

	return res
}
