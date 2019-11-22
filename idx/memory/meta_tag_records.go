package memory

import (
	"sync"

	"github.com/grafana/metrictank/errors"
	"github.com/grafana/metrictank/expr/tagquery"
)

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

func (m *metaTagRecords) getMetaRecordById(id recordId) (tagquery.MetaTagRecord, bool) {
	m.RLock()
	defer m.RUnlock()

	record, ok := m.records[id]
	return record, ok
}

func (m *metaTagRecords) getMetaRecordsByRecordIds(recordIds map[recordId]struct{}) []tagquery.MetaTagRecord {
	var res []tagquery.MetaTagRecord

	m.RLock()
	defer m.RUnlock()

	for recordId := range recordIds {
		res = append(res, m.records[recordId])
	}

	return res
}

func (m *metaTagRecords) getMetaTagsByRecordIds(recordIds map[recordId]struct{}) tagquery.Tags {
	records := m.getMetaRecordsByRecordIds(recordIds)
	res := make(tagquery.Tags, 0, len(records))
	for _, record := range records {
		res = append(res, record.MetaTags...)
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
