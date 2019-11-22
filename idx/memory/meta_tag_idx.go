package memory

import (
	"fmt"
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
	idLookup func(uint32, tagquery.Query, func(chan schema.MKey))
}

func newMetaTagIndex(idLookup func(uint32, tagquery.Query, func(chan schema.MKey))) *metaTagIdx {
	return &metaTagIdx{
		byOrg:    make(map[uint32]*orgMetaTagIdx),
		idLookup: idLookup,
	}
}

func (m *metaTagIdx) stop() {

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

	idLookup := func(query tagquery.Query, callback func(chan schema.MKey)) {
		m.idLookup(orgId, query, callback)
	}
	idx = newOrgMetaTagIndex(idLookup)

	m.byOrg[orgId] = idx

	return idx
}

type orgMetaTagIdx struct {
	// swapMutex is only used to ensure that no two upsert/swap operations run concurrently
	swapMutex sync.Mutex

	tags     *metaTagHierarchy
	records  *metaTagRecords
	enricher *metaTagEnricher
}

type idLookup func(tagquery.Query, func(chan schema.MKey))

func newOrgMetaTagIndex(idLookup idLookup) *orgMetaTagIdx {
	return &orgMetaTagIdx{
		tags:     newMetaTagHierarchy(),
		records:  newMetaTagRecords(),
		enricher: newEnricher(idLookup),
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

	id, oldId, oldRecord, err := idx.records.upsert(upsertRecord)
	if err != nil {
		return err
	}

	// check if the upsert has replaced a previously existing record
	if oldId > 0 {
		// if so we remove all references to it from the enricher
		// and from the meta tag index
		idx.enricher.delMetaRecord(oldId)
		idx.tags.deleteRecord(oldRecord.MetaTags, oldId)
	}

	// add the newly inserted meta record into the enricher and the
	// meta tag index
	idx.enricher.addMetaRecord(id, query)
	idx.tags.insertRecord(upsertRecord.MetaTags, id)

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

	var query tagquery.Query
	var recordsModified, recordsAdded, recordsPruned uint32
	for i, status := range recordComparison {
		if status.recordExists && status.isEqual {
			//  record does not need any modification
			continue
		}

		if status.recordExists {
			// record exists, but its meta tags need to be updated,
			// we first delete it and then re-add it
			recordsModified++
			idx.enricher.delMetaRecord(status.currentId)
			idx.tags.deleteRecord(status.currentMetaTags, status.currentId)
		} else {
			// record does not exist, so it will be added
			recordsAdded++
		}

		newRecordId, _, _, err := idx.records.upsert(newRecords[i])
		if err != nil {
			log.Errorf("Error when upserting meta record (%q/%q): %s", newRecords[i].Expressions.Strings(), newRecords[i].MetaTags.Strings(), err.Error())
			continue
		}

		query, err = tagquery.NewQuery(newRecords[i].Expressions, 0)
		if err != nil {
			log.Errorf("Invalid record (%q/%q): %s", newRecords[i].Expressions.Strings(), newRecords[i].MetaTags.Strings(), err)
			continue
		}
		idx.enricher.addMetaRecord(newRecordId, query)
		idx.tags.insertRecord(newRecords[i].MetaTags, newRecordId)

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
			idx.enricher.delMetaRecord(recordId)
			idx.tags.deleteRecord(record.MetaTags, recordId)
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
