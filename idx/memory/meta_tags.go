package memory

import (
	"sync"

	"github.com/grafana/metrictank/errors"
	"github.com/grafana/metrictank/expr/tagquery"
	"github.com/grafana/metrictank/schema"
	"github.com/grafana/metrictank/stats"
	log "github.com/sirupsen/logrus"
)

var (
	// the collision avoidance window defines how many times we try to find a higher
	// slot that's free if two record hashes collide
	collisionAvoidanceWindow = uint32(1024)

	// metric idx.memory.meta-tags.enricher.ops.metrics-filtered-by-meta-record.accepted is a counter of metrics getting accepted by meta record filters
	enricherMetricsFilteredByMetaRecordAccepted = stats.NewCounter32("idx.memory.meta-tags.enricher.ops.metrics-filtered-by-meta-record.accepted")

	// metric idx.memory.meta-tags.enricher.ops.metrics-filtered-by-meta-record.rejected is a counter of metrics getting rejected by meta record filters
	enricherMetricsFilteredByMetaRecordRejected = stats.NewCounter32("idx.memory.meta-tags.enricher.ops.metrics-filtered-by-meta-record.rejected")

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
	// filters to check whether a metric definition matches the requirements of a meta record,
	// keyed by the according meta record id
	filtersByRecord map[recordId]tagquery.MetricDefinitionFilter
	// mapping from metric key to list of meta record ids, used to do the enrichment
	recordsByMetric map[schema.Key]map[recordId]struct{}
	// lookup table from event types to event handlers used by the event queue consumer routine
	eventHandlers map[enricherEventType]func(interface{})
	// the event queue, all state changing operations get pushed through this queue
	eventQueue chan enricherEvent
}

type enricherEventType uint8

const (
	addMetric enricherEventType = iota
	delMetric
	addMetaRecord
	delMetaRecord
)

type enricherEvent struct {
	// eventType gets looked up from the eventHandlers map to obtain the handler for the payload
	eventType enricherEventType
	payload   interface{}
}

func newEnricher() *metaTagEnricher {
	res := &metaTagEnricher{
		filtersByRecord: make(map[recordId]tagquery.MetricDefinitionFilter),
		recordsByMetric: make(map[schema.Key]map[recordId]struct{}),
	}

	// eventHandlers gets instantiated as a struct member so it can be modified for testability
	res.eventHandlers = map[enricherEventType]func(interface{}){
		addMetric:     res._addMetric,
		delMetric:     res._delMetric,
		addMetaRecord: res._addMetaRecord,
		delMetaRecord: res._delMetaRecord,
	}

	res.start()
	return res
}

// start creates the go routine which consumes the event queue and processes received events
func (e *metaTagEnricher) start() {
	e.eventQueue = make(chan enricherEvent, 1000)

	e.wg.Add(1)
	go func() {
		defer e.wg.Done()

		var handler func(interface{})
		var ok bool

		for event := range e.eventQueue {
			handler, ok = e.eventHandlers[event.eventType]
			if !ok {
				log.Errorf("metaTagEnricher received unknown event type: %d", event.eventType)
				continue
			}

			// handlers are responsible for type casting the payload
			handler(event.payload)
		}
	}()
}

func (e *metaTagEnricher) stop() {
	close(e.eventQueue)
	e.wg.Wait()
}

func (e *metaTagEnricher) addMetric(md *schema.MetricDefinition, indexLock *sync.RWMutex) {
	e.eventQueue <- enricherEvent{
		eventType: addMetric,
		payload: struct {
			md   *schema.MetricDefinition
			lock *sync.RWMutex
		}{md: md, lock: indexLock},
	}
}

// _addMetric is used by the event queue consumer to add metrics to the enricher.
// this operation requires the metric definition to get passed through each of the
// filter functions generated from the defined meta records to determine which meta
// records shall get assigned to added metric.
// this operation can be relatively expensive compared to all other operations of
// the enricher, so it is important that it only gets called asynchronously via the
// event queue.
func (e *metaTagEnricher) _addMetric(payload interface{}) {
	data := payload.(struct {
		md   *schema.MetricDefinition
		lock *sync.RWMutex
	})
	recordIds := make(map[recordId]struct{})

	// enricher read lock is not necessary here because the event queue processor is
	// single threaded and there are no state-modifying operations which do not go
	// through the event queue.
	// however, the main index read lock is necessary because the filter functions
	// may do lookups on the index.
	if data.lock != nil {
		data.lock.RLock()
	}

	var accepted, rejected uint32
	for record, filter := range e.filtersByRecord {
		if filter(data.md.Id, data.md.Name, data.md.Tags) == tagquery.Pass {
			recordIds[record] = struct{}{}
			accepted++
		} else {
			rejected++
		}
	}
	if data.lock != nil {
		data.lock.RUnlock()
	}

	enricherMetricsFilteredByMetaRecordAccepted.AddUint32(accepted)
	enricherMetricsFilteredByMetaRecordRejected.AddUint32(rejected)

	if len(recordIds) > 0 {
		e.Lock()
		e.recordsByMetric[data.md.Id.Key] = recordIds
		e.Unlock()
		enricherMetricsWithMetaRecords.SetUint32(uint32(len(e.recordsByMetric)))
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

func (e *metaTagEnricher) addMetaRecord(id recordId, filter tagquery.MetricDefinitionFilter, keys []schema.Key) {
	e.eventQueue <- enricherEvent{
		eventType: addMetaRecord,
		payload: struct {
			id     recordId
			filter tagquery.MetricDefinitionFilter
			keys   []schema.Key
		}{id: id, filter: filter, keys: keys},
	}
}

func (e *metaTagEnricher) _addMetaRecord(payload interface{}) {
	data := payload.(struct {
		id     recordId
		filter tagquery.MetricDefinitionFilter
		keys   []schema.Key
	})

	e.Lock()
	e.filtersByRecord[data.id] = data.filter
	e.Unlock()
	enricherKnownMetaRecords.SetUint32(uint32(len(e.filtersByRecord)))

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

	// before deleting the meta record from the e.filtersByRecord map we
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
	delete(e.filtersByRecord, data.id)
	e.Unlock()
	enricherKnownMetaRecords.SetUint32(uint32(len(e.filtersByRecord)))
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
type metaTagIndex map[string]metaTagValue

func (m metaTagIndex) deleteRecord(keyValue tagquery.Tag, id recordId) {
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

func (m metaTagIndex) insertRecord(keyValue tagquery.Tag, id recordId) {
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
func (m metaTagIndex) getMetaRecordIdsByExpression(expr tagquery.Expression, invertSetOfMetaRecords bool) []recordId {
	if expr.OperatesOnTag() {
		return m.getByTag(expr, invertSetOfMetaRecords)
	}
	return m.getByTagValue(expr, invertSetOfMetaRecords)
}

func (m metaTagIndex) getByTag(expr tagquery.Expression, invertSetOfMetaRecords bool) []recordId {
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

func (m metaTagIndex) getByTagValue(expr tagquery.Expression, invertSetOfMetaRecords bool) []recordId {
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
