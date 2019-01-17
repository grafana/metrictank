package memory

import (
	"fmt"
	"hash/fnv"
	"math"
	"reflect"
	"sort"
	"strings"
	"sync"

	jump "github.com/dgryski/go-jump"
)

type metaTagRecord struct {
	metaTags []kv
	queries  []expression
}

// list of meta records keyed by random unique identifier
// key needs to be somehow generated, could be a completely random number
type metaTagRecords struct {
	sync.RWMutex
	records map[uint32]metaTagRecord
}

func NewMetaTagRecords() *metaTagRecords {
	return &metaTagRecords{
		records: make(map[uint32]metaTagRecord),
	}
}

// index structure keyed by key -> value -> meta record
type metaTagValue map[string]uint32
type metaTagIndex map[string]metaTagValue

// metaTagRecordFromStrings takes two slices of strings, parses them and returns a metaTagRecord
// The first slice of strings are the meta tags & values
// The second slice is the tag query expressions which the meta key & values refer to
// On parsing error the second returned value is an error, otherwise it is nil
func metaTagRecordFromStrings(metaTags []string, tagQueryExpressions []string) (metaTagRecord, error) {
	record := metaTagRecord{
		metaTags: make([]kv, 0, len(metaTags)),
		queries:  make([]expression, 0, len(tagQueryExpressions)),
	}
	if len(metaTags) == 0 {
		return record, fmt.Errorf("Requiring at least one meta tag, 0 given")
	}
	if len(tagQueryExpressions) == 0 {
		return record, fmt.Errorf("Requiring at least one tag query expression, 0 given")
	}

	for _, tag := range metaTags {
		tagSplits := strings.SplitN(tag, "=", 2)
		if len(tagSplits) < 2 {
			return record, fmt.Errorf("Missing \"=\" sign in tag %s", tag)
		}

		record.metaTags = append(record.metaTags, kv{key: tagSplits[0], value: tagSplits[1]})
	}

	for _, query := range tagQueryExpressions {
		parsed, err := parseExpression(query)
		if err != nil {
			return record, err
		}
		record.queries = append(record.queries, parsed)
	}

	return record, nil
}

// hashMetaTagRecord generates a uint32 hash from the entire metaTagRecord's content
func (m *metaTagRecord) hashMetaTagRecord() uint32 {
	m.sortMetaTags()
	metaTagHash := m.hashMetaTags()

	m.sortQueries()
	queryHash := m.hashQueries()

	// use jump hash for better distribution
	// we want distribution to avoid collisions and to find the next
	// free ID faster in case there was a collision
	return uint32(jump.Hash(uint64(metaTagHash)<<32|uint64(queryHash), math.MaxUint32))
}

// hashMetaTags generates a hash of all the meta tags in the record
func (m *metaTagRecord) hashMetaTags() uint32 {
	builder := strings.Builder{}
	for i, metaTag := range m.metaTags {
		if i > 0 {
			builder.WriteString(";")
		}
		builder.WriteString(metaTag.String())
	}
	h := fnv.New32a()
	h.Write([]byte(builder.String()))
	return h.Sum32()
}

// hashQueries generates a hash of all the queries in the record
func (m *metaTagRecord) hashQueries() uint32 {
	builder := strings.Builder{}
	for i, query := range m.queries {
		if i > 0 {
			builder.WriteString(";")
		}
		builder.WriteString(query.String())
	}
	h := fnv.New32a()
	h.Write([]byte(builder.String()))
	return h.Sum32()
}

// Sort sorts all the tag expressions first by key, then by value, then by operator
func (m *metaTagRecord) sortMetaTags() {
	sort.Slice(m.metaTags, func(i, j int) bool {
		if m.metaTags[i].key == m.metaTags[j].key {
			return m.metaTags[i].value < m.metaTags[j].key
		}
		return m.metaTags[i].key < m.metaTags[j].key
	})
}

// SortQueries sorts all the queries first by key and then by value
func (m *metaTagRecord) sortQueries() {
	sort.Slice(m.queries, func(i, j int) bool {
		if m.queries[i].key == m.queries[j].key {
			if m.queries[i].value == m.queries[j].value {
				return m.queries[i].operator < m.queries[j].operator
			}
			return m.queries[i].value < m.queries[j].value
		}
		return m.queries[i].key < m.queries[j].key
	})
}

// metaTagRecordUpsert inserts or updates a meta tag record according to the given specifications
// it uses the set of tag query expressions as the identity of the record, if a record with the
// same identity is already present then its meta tags gets updated to the specified ones
// If an update was performed the first returned value is a reference to the old record, otherwise
// the first returned value is nil
// In case of an error the second returned value is an error message, otherwise it is nil
func (m *metaTagRecords) metaTagRecordUpsert(metaTags []string, metricTagQueryExpressions []string) (*metaTagRecord, error) {
	record, err := metaTagRecordFromStrings(metaTags, metricTagQueryExpressions)
	if err != nil {
		return nil, err
	}

	hash := record.hashMetaTagRecord()
	var updatedRecord *metaTagRecord

	for {
		m.RLock()
		// first of all, verify that the same record doesn't exist already
		if existingRecord, ok := m.records[hash]; ok {
			if reflect.DeepEqual(record, existingRecord) {
				m.RUnlock()
				// an equal record is already present, directly exit without error
				return nil, nil
			}
		}

		// check if there is another meta record which has the same tag query expressions
		// in such a case we need to update it instead of adding a new one
		queryHash := record.hashQueries()
		updateRecordID := uint32(0)
		for i, existingRecord := range m.records {
			if queryHash == existingRecord.hashQueries() {
				// verify that this record is not equal to the one we want to add
				if reflect.DeepEqual(record, existingRecord) {
					m.RUnlock()
					// an equal record is already present, directly exit without error
					return nil, nil
				}

				// verify that the two records have the same queries and there was no hash collision
				if reflect.DeepEqual(record.queries, existingRecord.queries) {
					// we will have to update this record's meta tags
					updateRecordID = i
					break
				}
			}
		}

		// if this is not going to be an update operation then we'll need to add a new
		// record, so make sure there is no hash collision with an existing record
		if updateRecordID == 0 {
			// keep increasing until we find the next free ID where we don't collide
			// with an already existing record
			for _, ok := m.records[hash]; ok; hash++ {
			}
		}

		// switch to write lock in order to execute the previously planned operation
		m.RUnlock()
		m.Lock()

		// insert a new record if updateRecordID is 0, otherwise update the record at that ID
		if updateRecordID == 0 {
			// ensure that no other record has been inserted at this ID while
			// we switched to the write lock
			if _, ok := m.records[hash]; ok {
				// another record with this ID already exists, start over again
				m.Unlock()
				continue
			}
		} else {
			// ensure that the tag expressions at the given ID still match the ones
			// of the new record, and also that this record still exists
			if existingRecord, ok := m.records[updateRecordID]; ok {
				if !reflect.DeepEqual(record.hashQueries(), existingRecord.hashQueries()) {
					// the tag expressions at the given ID do not match the ones of
					// the new record anymore, start over again
					m.Unlock()
					continue
				}

				// delete record at this ID, its replacement will be inserted with a new ID
				delete(m.records, updateRecordID)
				updatedRecord = &existingRecord
			} else {
				// there is no record at the given ID anymore, start over again
				m.Unlock()
				continue
			}
		}

		m.records[hash] = record
		m.Unlock()
		break
	}

	return updatedRecord, nil
}
