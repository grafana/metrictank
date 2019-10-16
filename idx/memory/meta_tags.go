package memory

import (
	"sort"
	"sync/atomic"
	"unsafe"

	"github.com/grafana/metrictank/util"

	"github.com/grafana/metrictank/errors"
	"github.com/grafana/metrictank/expr/tagquery"
	"github.com/grafana/metrictank/schema"
	lru "github.com/hashicorp/golang-lru"
)

// the collision avoidance window defines how many times we try to find a higher
// slot that's free if two record hashes collide
var collisionAvoidanceWindow = uint32(1024)

type recordId uint32

// list of meta records keyed by a unique identifier used as ID
type metaTagRecords struct {
	records  map[recordId]tagquery.MetaTagRecord
	enricher unsafe.Pointer
}

func newMetaTagRecords() *metaTagRecords {
	return &metaTagRecords{
		records: make(map[recordId]tagquery.MetaTagRecord),
	}
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
	// after altering meta records we need to reinstantiate the enricher the next time we want to use it
	defer atomic.StorePointer(&m.enricher, nil)

	record.Expressions.Sort()

	id := recordId(record.HashExpressions())

	var oldRecord *tagquery.MetaTagRecord
	var oldId recordId

	// loop over existing records, starting from id, trying to find one that has
	// the exact same queries as the one we're upserting
	for i := uint32(0); i < collisionAvoidanceWindow; i++ {
		if existingRecord, ok := m.records[id+recordId(i)]; ok {
			if record.EqualExpressions(&existingRecord) {
				oldRecord = &existingRecord
				oldId = id + recordId(i)
				delete(m.records, oldId)
				break
			}
		}
	}

	if !record.HasMetaTags() {
		return 0, &record, oldId, oldRecord, nil
	}

	record.MetaTags.Sort()

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

func (m *metaTagRecords) getEnricher(lookup tagquery.IdTagLookup) *enricher {
	res := (*enricher)(atomic.LoadPointer(&m.enricher))
	if res != nil {
		return res
	}

	// if no enricher is present yet, then we instantiate one and store its reference
	// to reuse it later.
	// there is an unlikely race condition where we receive multiple calls to FindByTag
	// from multiple requests at the same time, which would result in multiple enrichers
	// getting instantiated. in such a case the last one would overwrite the previous
	// ones and there shouldn't be any issues. this case is very unlikely, so it doesn't
	// seem to be worth it to add a lock for that.

	// if provided size is valid, it's not possible for lru.New to return an error
	cache, _ := lru.New(enrichmentCacheSize)
	res = &enricher{
		filters: make([]tagquery.MetricDefinitionFilter, len(m.records)),
		tags:    make([]tagquery.Tags, len(m.records)),
		cache:   cache,
	}

	i := 0
	for _, record := range m.records {
		res.filters[i] = record.GetMetricDefinitionFilter(lookup)
		res.tags[i] = record.MetaTags
		i++
	}

	// to atomically store a pointer, unfortunately we need to use unsafe.Pointer
	atomic.StorePointer(&m.enricher, (unsafe.Pointer)(res))

	return res
}

func (m *metaTagRecords) hashRecords() uint32 {
	i := 0
	recordIds := make([]recordId, len(m.records))
	for recordId := range m.records {
		recordIds[i] = recordId
		i++
	}

	sort.Slice(recordIds, func(i, j int) bool {
		return recordIds[i] < recordIds[j]
	})

	h := tagquery.QueryHash()
	var record tagquery.MetaTagRecord
	var recordHash uint64
	for _, recordId := range recordIds {
		record = m.records[recordId]
		recordHash = record.HashRecord()

		h.Write([]byte{
			byte(recordHash),
			byte(recordHash >> 8),
			byte(recordHash >> 16),
			byte(recordHash >> 24),
			byte(recordHash >> 32),
			byte(recordHash >> 40),
			byte(recordHash >> 48),
			byte(recordHash >> 56),
		})
	}

	return h.Sum32()
}

type enricher struct {
	filters []tagquery.MetricDefinitionFilter
	tags    []tagquery.Tags
	cache   *lru.Cache
}

func (e *enricher) enrich(id schema.MKey, name string, tags []string) tagquery.Tags {
	h := util.NewFnv64aStringWriter()
	h.WriteString(name)
	for i := range tags {
		h.WriteString(";")
		h.WriteString(tags[i])
	}
	sum := h.Sum64()

	cachedRes, ok := e.cache.Get(sum)
	if ok {
		return cachedRes.(tagquery.Tags)
	}

	var res tagquery.Tags
	var matches []int
	for i := range e.filters {
		if e.filters[i](id, name, tags) == tagquery.Pass {
			res = append(res, e.tags[i]...)
			matches = append(matches, i)
		}
	}

	e.cache.Add(sum, res)

	return res
}

// index structure keyed by tag -> value -> list of meta record IDs
type metaTagValue map[string][]recordId
type metaTagIndex map[string]metaTagValue

func (m metaTagIndex) deleteRecord(keyValue tagquery.Tag, id recordId) {
	if values, ok := m[keyValue.Key]; ok {
		if ids, ok := values[keyValue.Value]; ok {
			for i := 0; i < len(ids); i++ {
				if ids[i] == id {
					// no need to keep the order
					ids[i] = ids[len(ids)-1]
					values[keyValue.Value] = ids[:len(ids)-1]

					// no id should ever be present more than once
					return
				}
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
