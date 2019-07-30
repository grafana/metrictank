package memory

import (
	"hash"
	"hash/fnv"
	"strings"

	"github.com/grafana/metrictank/errors"
	"github.com/grafana/metrictank/expr/tagquery"
)

// the collision avoidance window defines how many times we try to find a higher
// slot that's free if two record hashes collide
var collisionAvoidanceWindow = uint32(1024)

// the function we use to get the hash for hashing the meta records
// it can be replaced for mocking in tests
var queryHash func() hash.Hash32

func init() {
	queryHash = fnv.New32a
}

// list of meta records keyed by a unique identifier used as ID
type metaTagRecords map[recordId]tagquery.MetaTagRecord

type recordId uint32

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
func (m metaTagRecords) upsert(record tagquery.MetaTagRecord) (recordId, *tagquery.MetaTagRecord, recordId, *tagquery.MetaTagRecord, error) {
	id := m.hashMetaTagRecord(record)
	var oldRecord *tagquery.MetaTagRecord
	var oldId recordId

	// loop over existing records, starting from id, trying to find one that has
	// the exact same queries as the one we're upserting
	for i := uint32(0); i < collisionAvoidanceWindow; i++ {
		if existingRecord, ok := m[id+recordId(i)]; ok {
			if record.EqualExpressions(&existingRecord) {
				oldRecord = &existingRecord
				oldId = id + recordId(i)
				delete(m, oldId)
				break
			}
		}
	}

	if !record.HasMetaTags() {
		return 0, &record, oldId, oldRecord, nil
	}

	// now find the best position to insert the new/updated record, starting from id
	for i := uint32(0); i < collisionAvoidanceWindow; i++ {
		// if we find a free slot, then insert the new record there
		if _, ok := m[id]; !ok {
			m[id] = record

			return id, &record, oldId, oldRecord, nil
		}

		id++
	}

	return 0, nil, 0, nil, errors.NewInternal("Could not find a free ID to insert record")
}

// hashMetaTagRecord generates a hash of all the queries in the record
func (m *metaTagRecords) hashMetaTagRecord(record tagquery.MetaTagRecord) recordId {
	builder := strings.Builder{}
	for _, query := range record.Expressions {
		query.StringIntoBuilder(&builder)

		// trailing ";" doesn't matter, this is only hash input
		builder.WriteString(";")
	}

	h := queryHash()
	h.Write([]byte(builder.String()))
	return recordId(h.Sum32())
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
