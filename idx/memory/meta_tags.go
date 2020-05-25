package memory

import (
	"context"
	"fmt"
	"regexp"
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

type metaTagIdx struct {
	sync.RWMutex
	byOrg    map[uint32]*orgMetaTagIdx
	idLookup func(uint32, tagquery.Query, func([]schema.MKey))
}

func newMetaTagIndex(idLookup func(uint32, tagquery.Query, func([]schema.MKey))) *metaTagIdx {
	return &metaTagIdx{
		byOrg:    make(map[uint32]*orgMetaTagIdx),
		idLookup: idLookup,
	}
}

func (m *metaTagIdx) stop() {
	m.Lock()
	for _, idx := range m.byOrg {
		idx.enricher.stop()
	}
	m.byOrg = make(map[uint32]*orgMetaTagIdx)
	m.Unlock()
}

func (m *metaTagIdx) getOrgMetaTagIndex(orgId uint32) *orgMetaTagIdx {
	m.RLock()
	idx := m.byOrg[orgId]
	m.RUnlock()
	if idx != nil {
		return idx
	}

	m.Lock()
	defer m.Unlock()

	idx = m.byOrg[orgId]
	if idx != nil {
		return idx
	}

	idx = newOrgMetaTagIndex()

	m.byOrg[orgId] = idx

	return idx
}

type orgMetaTagIdx struct {
	// swapMutex is only used to ensure that no two upsert/swap operations run concurrently
	swapMutex sync.Mutex

	hierarchy *metaTagHierarchy
	records   *metaTagRecords
	enricher  *metaTagEnricher
}

func newOrgMetaTagIndex() *orgMetaTagIdx {
	return &orgMetaTagIdx{
		hierarchy: newMetaTagHierarchy(),
		records:   newMetaTagRecords(),
		enricher:  newEnricher(),
	}
}

func (m *orgMetaTagIdx) getMetaTagsById(id schema.Key) tagquery.Tags {
	return m.records.getMetaTagsByRecordIds(m.enricher.enrich(id))
}

// MetaTagRecordUpsert inserts or updates a meta record, depending on whether
// it already exists or is new. The identity of a record is determined by its
// queries, if the set of queries in the given record already exists in another
// record, then the existing record will be updated, otherwise a new one gets
// created.
func (m *metaTagIdx) MetaTagRecordUpsert(orgId uint32, upsertRecord tagquery.MetaTagRecord) error {
	if !TagSupport || !MetaTagSupport {
		log.Warn("memory-idx: received tag/meta-tag query, but that feature is disabled")
		return errors.NewBadRequest("Tag/Meta-Tag support is disabled")
	}

	// expressions need to be sorted because the unique ID of a meta record is
	// its sorted set of expressions
	upsertRecord.Expressions.Sort()

	// initialize query in preparation to execute it once we have the swapMutex
	// doing struct instantiations before acquiring lock to keep mutex time short
	query, err := tagquery.NewQuery(upsertRecord.Expressions, 0)
	if err != nil {
		return fmt.Errorf("Invalid record with expressions/meta tags: %q/%q", upsertRecord.Expressions, upsertRecord.MetaTags)
	}

	idx := m.getOrgMetaTagIndex(orgId)

	idx.swapMutex.Lock()
	defer idx.swapMutex.Unlock()

	newRecordId, oldRecordId, oldRecord, err := idx.records.upsert(upsertRecord)
	if err != nil {
		return err
	}

	// check if the upsert has replaced a previously existing record
	if oldRecordId > 0 {
		// if so we remove all references to it from the enricher
		// and from the meta tag index. we can reuse the already existing query
		// because the identity of a meta record is its query expressions,
		// so the new and the old record must have the same expressions
		m.idLookup(orgId, query, func(metricIds []schema.MKey) {
			idx.enricher.delMetaRecord(oldRecordId, query, metricIds)
		})
		idx.hierarchy.deleteRecord(oldRecord.MetaTags, oldRecordId)
	}

	// lookup metrics from main index by the given query, then update the
	// enricher with the new record id and the associated metrics
	m.idLookup(orgId, query, func(metricIds []schema.MKey) {
		idx.enricher.addMetaRecord(newRecordId, query, metricIds)
	})

	// add the newly inserted meta record to the meta tag index
	idx.hierarchy.insertRecord(upsertRecord.MetaTags, newRecordId)

	return nil
}

func (m *metaTagIdx) MetaTagRecordSwap(orgId uint32, newRecords []tagquery.MetaTagRecord) error {
	if !TagSupport || !MetaTagSupport {
		log.Warn("memory-idx: received a tag/meta-tag query, but that feature is disabled")
		return errors.NewBadRequest("Tag/Meta-Tag support is disabled")
	}

	metaRecordSwapExecuting.Inc()

	log.Infof("memory-idx: Initiating Swap with %d records for org %d", len(newRecords), orgId)

	// recordIdsToKeep contains the records that should not
	// get pruned at the end of this swap
	recordIdsToKeep := make(map[recordId]struct{}, len(newRecords))
	var recordsToUpsert uint32

	idx := m.getOrgMetaTagIndex(orgId)
	idx.swapMutex.Lock()
	defer idx.swapMutex.Unlock()

	for i := range newRecords {
		newRecords[i].Expressions.Sort()
		newRecords[i].MetaTags.Sort()
	}

	recordComparison := idx.records.compareRecords(newRecords)
	for _, status := range recordComparison {
		if status.recordExists && status.isEqual {
			recordIdsToKeep[status.currentId] = struct{}{}
			continue
		}
		recordsToUpsert++
	}

	log.Infof("memory-idx: After diff against existing meta records for org %d, going to upsert %d, %d remain unchanged", orgId, recordsToUpsert, len(recordIdsToKeep))
	recordsUnchanged := uint32(len(recordIdsToKeep))

	var recordsModified, recordsAdded, recordsPruned uint32
	for i, status := range recordComparison {
		if status.recordExists && status.isEqual {
			//  record does not need any modification
			continue
		}

		query, err := tagquery.NewQuery(newRecords[i].Expressions, 0)
		if err != nil {
			log.Errorf("Invalid record (%q/%q): %s", newRecords[i].Expressions.Strings(), newRecords[i].MetaTags.Strings(), err)
			continue
		}

		if status.recordExists {
			// record exists, but its meta tags need to be updated,
			// we first delete it and then re-add it
			recordsModified++
			// we can use the query which has been instantiated from the new
			// record because the identity of a meta record is defined by its
			// expressions, so the old and the new record both must have the
			// same expressions.
			m.idLookup(orgId, query, func(metricIds []schema.MKey) {
				idx.enricher.delMetaRecord(status.currentId, query, metricIds)
			})
			idx.hierarchy.deleteRecord(status.currentMetaTags, status.currentId)
		} else {
			// record does not exist, so it will be added
			recordsAdded++
		}

		newRecordId, _, _, err := idx.records.upsert(newRecords[i])
		if err != nil {
			log.Errorf("Error when upserting meta record (%q/%q): %s", newRecords[i].Expressions.Strings(), newRecords[i].MetaTags.Strings(), err.Error())
			continue
		}

		m.idLookup(orgId, query, func(metricIds []schema.MKey) {
			idx.enricher.addMetaRecord(newRecordId, query, metricIds)
		})
		idx.hierarchy.insertRecord(newRecords[i].MetaTags, newRecordId)

		// adding the new record id to recordIdsToKeep to prevent that
		// it gets pruned further down
		recordIdsToKeep[newRecordId] = struct{}{}
	}

	// if the number of meta tag records is equal to the number of record
	// ids to keep, and we've already ensured that the meta tag records are
	// all up2date, then there's nothing to prune
	if idx.records.length() != len(recordIdsToKeep) {
		var pruned map[recordId]tagquery.MetaTagRecord
		toPrune := idx.records.getPrunable(recordIdsToKeep)
		if len(toPrune) > 0 {
			log.Infof("memory-idx: Going to prune %d meta records for org %d", len(toPrune), orgId)
			recordsPruned = uint32(len(toPrune))

			// we can assume that the toPrune list is still correct because we're
			// holding the metaRecordLock
			pruned = make(map[recordId]tagquery.MetaTagRecord, len(toPrune))
			idx.records.prune(toPrune, pruned)
		}

		// remove all references to the pruned meta records from the meta
		// tag index and the enricher
		for recordId, record := range pruned {
			query, err := tagquery.NewQuery(record.Expressions, 0)
			if err != nil {
				log.Errorf("Invalid record to prune, cannot instantiate query for (%q/%q): %s", record.Expressions.Strings(), record.MetaTags.Strings(), err)
				continue
			}
			m.idLookup(orgId, query, func(metricIds []schema.MKey) {
				idx.enricher.delMetaRecord(recordId, query, metricIds)
			})
			idx.hierarchy.deleteRecord(record.MetaTags, recordId)
		}
	}

	metaRecordSwapUnchanged.AddUint32(recordsUnchanged)
	metaRecordSwapAdded.AddUint32(recordsAdded)
	metaRecordSwapModified.AddUint32(recordsModified)
	metaRecordSwapPruned.AddUint32(recordsPruned)

	return nil
}

func (m *metaTagIdx) MetaTagRecordList(orgId uint32) []tagquery.MetaTagRecord {
	if !TagSupport || !MetaTagSupport {
		log.Warn("memory-idx: received a tag/meta-tag query, but that feature is disabled")
		return nil
	}

	metaTagIdx := m.getOrgMetaTagIndex(orgId)
	if metaTagIdx == nil {
		return nil
	}

	return metaTagIdx.records.listRecords()
}

type recordId uint32

// list of meta records keyed by a unique identifier used as ID
type metaTagRecords struct {
	sync.RWMutex
	records map[recordId]tagquery.MetaTagRecord
}

func newMetaTagRecords() *metaTagRecords {
	return &metaTagRecords{
		records: make(map[recordId]tagquery.MetaTagRecord),
	}
}

func (m *metaTagRecords) length() int {
	m.RLock()
	defer m.RUnlock()

	return len(m.records)
}

func (m *metaTagRecords) prune(toPrune map[recordId]struct{}, pruned map[recordId]tagquery.MetaTagRecord) {
	m.Lock()
	defer m.Unlock()

	for recordId := range toPrune {
		pruned[recordId] = m.records[recordId]
		delete(m.records, recordId)
	}
}

func (m *metaTagRecords) getPrunable(toKeep map[recordId]struct{}) map[recordId]struct{} {
	m.RLock()
	defer m.RUnlock()

	toPrune := make(map[recordId]struct{}, len(m.records)-len(toKeep))
	for recordId := range m.records {
		if _, ok := toKeep[recordId]; !ok {
			toPrune[recordId] = struct{}{}
		}
	}
	return toPrune
}

func (m *metaTagRecords) getMetaRecordById(recordId recordId) (tagquery.MetaTagRecord, bool) {
	m.RLock()
	defer m.RUnlock()
	record, ok := m.records[recordId]
	return record, ok
}

func (m *metaTagRecords) getMetaTagsByRecordIds(recordIds map[recordId]struct{}) tagquery.Tags {
	m.RLock()
	defer m.RUnlock()
	res := make(tagquery.Tags, 0, len(recordIds))
	for recordId := range recordIds {
		res = append(res, m.records[recordId].MetaTags...)
	}

	return res
}

// recordExists takes a meta record and checks if it exists
// the identity of a record is determined by its set of query expressions, so if there is
// any other record with the same query expressions this method returns the id, the record,
// and true. if it doesn't exist the third return value is false.
// it assumes that the expressions of the given record are sorted.
// it assumes that a read lock has already been acquired.
func (m *metaTagRecords) recordExists(record tagquery.MetaTagRecord) (recordId, tagquery.MetaTagRecord, bool) {
	id := recordId(record.HashExpressions())

	// loop over existing records, starting from id, trying to find one that has
	// the exact same queries as the one we're upserting
	for i := uint32(0); i < collisionAvoidanceWindow; i++ {
		checkingId := id + recordId(i)
		if existingRecord, ok := m.records[checkingId]; ok {
			if record.Expressions.Equal(existingRecord.Expressions) {
				return checkingId, existingRecord, true
			}
		}
	}

	return 0, tagquery.MetaTagRecord{}, false
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

func (m *metaTagRecords) compareRecords(records []tagquery.MetaTagRecord) []struct {
	currentId       recordId
	currentMetaTags tagquery.Tags
	recordExists    bool
	isEqual         bool
} {
	res := make([]struct {
		currentId       recordId
		currentMetaTags tagquery.Tags
		recordExists    bool
		isEqual         bool
	}, len(records))

	m.RLock()
	defer m.RUnlock()

	for i := range records {
		id, exists, equal := m.recordExistsAndIsEqual(records[i])
		res[i].currentId = id
		res[i].currentMetaTags = m.records[id].MetaTags
		res[i].recordExists = exists
		res[i].isEqual = equal
	}

	return res
}

func (m *metaTagRecords) listRecords() []tagquery.MetaTagRecord {
	m.RLock()
	defer m.RUnlock()

	res := make([]tagquery.MetaTagRecord, len(m.records))
	var i uint32
	for _, record := range m.records {
		res[i] = record
		i++
	}
	return res
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
func (m *metaTagRecords) upsert(record tagquery.MetaTagRecord) (recordId, recordId, tagquery.MetaTagRecord, error) {
	m.Lock()
	defer m.Unlock()

	record.Expressions.Sort()

	oldId, oldRecord, exists := m.recordExists(record)
	if exists {
		delete(m.records, oldId)
	}

	if !record.HasMetaTags() {
		return 0, oldId, oldRecord, nil
	}

	record.MetaTags.Sort()
	id := recordId(record.HashExpressions())

	// now find the best position to insert the new/updated record, starting from id
	for i := uint32(0); i < collisionAvoidanceWindow; i++ {
		// if we find a free slot, then insert the new record there
		if _, ok := m.records[id]; !ok {
			m.records[id] = record

			return id, oldId, oldRecord, nil
		}

		id++
	}

	return 0, 0, tagquery.MetaTagRecord{}, errors.NewInternal("Could not find a free ID to insert record")
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
				go func() {
					queryCtx.Run(tags, defById, nil, nil, idCh)
					close(idCh)
				}()
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

func (e *metaTagEnricher) addMetaRecord(id recordId, query tagquery.Query, metricIds []schema.MKey) {
	e.eventQueue <- enricherEvent{
		eventType: addMetaRecord,
		payload: struct {
			recordId  recordId
			query     tagquery.Query
			metricIds []schema.MKey
		}{recordId: id, query: query, metricIds: metricIds},
	}
}

func (e *metaTagEnricher) _addMetaRecord(payload interface{}) {
	data := payload.(struct {
		recordId  recordId
		query     tagquery.Query
		metricIds []schema.MKey
	})

	var added uint32

	e.Lock()
	for _, id := range data.metricIds {
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

func (e *metaTagEnricher) delMetaRecord(id recordId, query tagquery.Query, metricIds []schema.MKey) {
	e.eventQueue <- enricherEvent{
		eventType: delMetaRecord,
		payload: struct {
			recordId  recordId
			metricIds []schema.MKey
		}{recordId: id, metricIds: metricIds},
	}
}

func (e *metaTagEnricher) _delMetaRecord(payload interface{}) {
	data := payload.(struct {
		recordId  recordId
		metricIds []schema.MKey
	})

	e.Lock()
	for _, id := range data.metricIds {
		delete(e.recordsByMetric[id.Key], data.recordId)
		if len(e.recordsByMetric[id.Key]) == 0 {
			delete(e.recordsByMetric, id.Key)
		}
	}

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

// index structure keyed by tag -> value -> list of meta record IDs
type metaTagValue map[string][]recordId
type metaTagKeys map[string]metaTagValue
type metaTagHierarchy struct {
	sync.RWMutex
	tags metaTagKeys
}

func newMetaTagHierarchy() *metaTagHierarchy {
	return &metaTagHierarchy{tags: make(metaTagKeys)}
}

func (m *metaTagHierarchy) deleteRecord(tags tagquery.Tags, id recordId) {
	m.Lock()
	defer m.Unlock()

	for _, tag := range tags {
		if ids, ok := m.tags[tag.Key][tag.Value]; ok {
			for i := 0; i < len(ids); i++ {
				if ids[i] == id {
					// no need to keep the order
					ids[i] = ids[len(ids)-1]
					m.tags[tag.Key][tag.Value] = ids[:len(ids)-1]
					break
				}
			}
			if len(m.tags[tag.Key][tag.Value]) == 0 {
				delete(m.tags[tag.Key], tag.Value)
				if len(m.tags[tag.Key]) == 0 {
					delete(m.tags, tag.Key)
				}
			}
		}
	}
}

func (m *metaTagHierarchy) insertRecord(tags tagquery.Tags, id recordId) {
	m.Lock()
	defer m.Unlock()

	var values metaTagValue
	var ok bool

	for _, tag := range tags {
		if values, ok = m.tags[tag.Key]; !ok {
			values = make(metaTagValue)
			m.tags[tag.Key] = values
		}
		values[tag.Value] = append(values[tag.Value], id)
	}
}

func (m *metaTagHierarchy) updateExpressionCosts(costs []expressionCost, exprs tagquery.Expressions) {
	if len(costs) != len(exprs) {
		log.Warnf("metaTagHierarchy.UpdateExpressionCosts: Invalid pair of expression costs and expressions")
		return
	}

	m.RLock()
	defer m.RUnlock()

	for i := range costs {
		costs[i].metaTag = m.hasMatchesForExpression(exprs[i])
	}
}

func (m *metaTagHierarchy) hasMatchesForExpression(expr tagquery.Expression) bool {
	var res bool

	if expr.OperatesOnTag() {
		if expr.MatchesExactly() {
			_, res = m.tags[expr.GetKey()]
		} else {
			for key := range m.tags {
				if expr.ResultIsSmallerWhenInverted() == !expr.Matches(key) {
					res = true
					break
				}
			}
		}
	} else {
		if expr.MatchesExactly() {
			_, res = m.tags[expr.GetKey()][expr.GetValue()]
		} else {
			for value := range m.tags[expr.GetKey()] {
				if expr.ResultIsSmallerWhenInverted() == !expr.Matches(value) {
					res = true
					break
				}
			}
		}
	}

	return res
}

// getMetaRecordIdsByExpression takes an expression and a bool, it returns all meta record
// ids of the records which match it. the bool indicates whether the result set should be
// inverted. In some cases inverting the result set can minimize its size.
// Minimizing the size of the returned result set will lead to a faster expression evaluation,
// because less meta records will need to be checked against a given MetricDefinition.
// The caller, after receiving the result set, needs to be aware of whether the result set
// is inverted and interpret it accordingly.
func (m *metaTagHierarchy) getMetaRecordIdsByExpression(expr tagquery.Expression, invertSetOfMetaRecords bool) []recordId {
	if expr.OperatesOnTag() {
		return m.getByTag(expr, invertSetOfMetaRecords)
	}
	return m.getByTagValue(expr, invertSetOfMetaRecords)
}

func (m *metaTagHierarchy) getByTag(expr tagquery.Expression, invertSetOfMetaRecords bool) []recordId {
	recordSet := make(map[recordId]struct{})

	m.RLock()
	defer m.RUnlock()

	// optimization for simple "=" expressions
	if !invertSetOfMetaRecords && expr.MatchesExactly() {
		for _, records := range m.tags[expr.GetKey()] {
			for _, record := range records {
				recordSet[record] = struct{}{}
			}
		}
	} else {
		for key := range m.tags {
			if invertSetOfMetaRecords {
				if expr.Matches(key) {
					continue
				}
			} else {
				if !expr.Matches(key) {
					continue
				}
			}

			for _, records := range m.tags[key] {
				for _, record := range records {
					recordSet[record] = struct{}{}
				}
			}
		}
	}

	res := make([]recordId, len(recordSet))
	i := 0
	for record := range recordSet {
		res[i] = record
		i++
	}

	return res
}

func (m *metaTagHierarchy) getByTagValue(expr tagquery.Expression, invertSetOfMetaRecords bool) []recordId {
	recordSet := make(map[recordId]struct{})

	m.RLock()
	defer m.RUnlock()

	// optimization for simple "=" expressions
	if !invertSetOfMetaRecords && expr.MatchesExactly() {
		return m.tags[expr.GetKey()][expr.GetValue()]
	}

	for value, records := range m.tags[expr.GetKey()] {
		passes := expr.Matches(value)

		if invertSetOfMetaRecords {
			passes = !passes
		}

		if !passes {
			continue
		}

		for _, record := range records {
			recordSet[record] = struct{}{}
		}
	}

	res := make([]recordId, len(recordSet))
	i := 0
	for record := range recordSet {
		res[i] = record
		i++
	}

	return res
}

func (m *metaTagHierarchy) getTagValuesByRegex(key string, filter *regexp.Regexp) map[string][]recordId {
	res := make(map[string][]recordId)

	m.RLock()
	defer m.RUnlock()

	for value, recordIds := range m.tags[key] {
		if filter != nil && !filter.MatchString(value) {
			continue
		}

		res[value] = recordIds
	}

	return res
}

func (m *metaTagHierarchy) getTagsByPrefix(prefix string) []string {
	var res []string

	m.RLock()
	defer m.RUnlock()

	for tag := range m.tags {
		if len(prefix) > 0 && !strings.HasPrefix(tag, prefix) {
			continue
		}
		res = append(res, tag)
	}

	return res
}
func (m *metaTagHierarchy) getTagsByFilter(filter *regexp.Regexp) []string {
	var res []string

	m.RLock()
	defer m.RUnlock()

	for tag := range m.tags {
		if filter != nil && !filter.MatchString(tag) {
			continue
		}
		res = append(res, tag)
	}

	return res
}

func (m *metaTagHierarchy) getTagValuesByTagAndPrefix(tag, prefix string) []string {
	var res []string

	m.RLock()
	defer m.RUnlock()

	for value := range m.tags[tag] {
		if len(prefix) > 0 && !strings.HasPrefix(value, prefix) {
			continue
		}
		res = append(res, value)
	}

	return res
}
