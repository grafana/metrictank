package memory

import (
	"fmt"
	"hash"
	"hash/fnv"
	"reflect"
	"sort"
	"strings"

	"github.com/grafana/metrictank/idx"
)

// the collision avoidance window defines how many times we try to find a higher
// slot that's free if two record hashes collide
const collisionAvoidanceWindow = 3

// the function we use to get the hash for hashing the meta records
var queryHash func() hash.Hash32

func init() {
	// can be replaced for mocking in tests
	queryHash = fnv.New32a
}

type metaTagRecord struct {
	metaTags []kv
	queries  []expression
}

// list of meta records keyed by random unique identifier
// key needs to be somehow generated, could be a completely random number
type metaTagRecords map[uint32]metaTagRecord

// index structure keyed by key -> value -> meta record
type metaTagValue map[string][]uint32
type metaTagIndex map[string]metaTagValue

func (m metaTagIndex) deleteRecord(keyValue kv, hash uint32) {
	if values, ok := m[keyValue.key]; ok {
		if hashes, ok := values[keyValue.value]; ok {
			for i := 0; i < len(hashes); i++ {
				if hashes[i] == hash {
					// no need to keep the order
					hashes[i] = hashes[len(hashes)-1]
					values[keyValue.value] = hashes[:len(hashes)-1]
					return
				}
			}
		}
	}
}

func (m metaTagIndex) insertRecord(keyValue kv, hash uint32) {
	var values metaTagValue
	var ok bool

	if values, ok = m[keyValue.key]; !ok {
		values = make(metaTagValue)
		m[keyValue.key] = values
	}

	values[keyValue.value] = append(values[keyValue.value], hash)
}

// metaTagRecordFromStrings takes two slices of strings, parses them and returns a metaTagRecord
// The first slice of strings are the meta tags & values
// The second slice is the tag query expressions which the meta key & values refer to
// On parsing error the second returned value is an error, otherwise it is nil
func metaTagRecordFromStrings(metaTags []string, tagQueryExpressions []string) (metaTagRecord, error) {
	record := metaTagRecord{
		metaTags: make([]kv, 0, len(metaTags)),
		queries:  make([]expression, 0, len(tagQueryExpressions)),
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

func (m *metaTagRecord) metaTagStrings(builder *strings.Builder) []string {
	res := make([]string, len(m.metaTags))

	for i, tag := range m.metaTags {
		tag.stringIntoBuilder(builder)
		res[i] = builder.String()
		builder.Reset()
	}

	return res
}

func (m *metaTagRecord) queryStrings(builder *strings.Builder) []string {
	res := make([]string, len(m.queries))

	for i, query := range m.queries {
		query.stringIntoBuilder(builder)
		res[i] = builder.String()
		builder.Reset()
	}

	return res
}

// hashQueries generates a hash of all the queries in the record
func (m *metaTagRecord) hashQueries() uint32 {
	builder := strings.Builder{}
	for i, query := range m.queries {
		if i > 0 {
			builder.WriteString(";")
		}
		query.stringIntoBuilder(&builder)
	}
	h := queryHash()
	h.Write([]byte(builder.String()))
	return h.Sum32()
}

// sortQueries sorts all the queries first by key and then by value
func (m *metaTagRecord) sortQueries() {
	sort.Slice(m.queries, func(i, j int) bool {
		if m.queries[i].getKey() == m.queries[j].getKey() {
			if m.queries[i].getValue() == m.queries[j].getValue() {
				return m.queries[i].getOperator() < m.queries[j].getOperator()
			}
			return m.queries[i].getValue() < m.queries[j].getValue()
		}
		return m.queries[i].getKey() < m.queries[j].getKey()
	})
}

// matchesQueries compares another tag record's queries to this
// one's queries. Returns true if they are equal, otherwise false
// It is assumed that all the queries are already sorted
func (m *metaTagRecord) matchesQueries(other metaTagRecord) bool {
	return reflect.DeepEqual(m.queries, other.queries)
}

// hasMetaTags returns true if the meta tag record has one or more
// meta tags, otherwise it returns false
func (m *metaTagRecord) hasMetaTags() bool {
	return len(m.metaTags) > 0
}

func (m *metaTagRecord) testByQueries(def *idx.Archive) bool {
	for _, expr := range m.queries {
		res := expr.getFilter()(def)
		if res == pass {
			continue
		}

		if res == fail {
			return false
		}

		if res == none && expr.getDefaultDecision() != pass {
			return false
		}
	}

	return true
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
func (m metaTagRecords) upsert(metaTags []string, metricTagQueryExpressions []string) (uint32, *metaTagRecord, uint32, *metaTagRecord, error) {
	record, err := metaTagRecordFromStrings(metaTags, metricTagQueryExpressions)
	if err != nil {
		return 0, nil, 0, nil, err
	}

	record.sortQueries()
	hash := record.hashQueries()
	var oldRecord *metaTagRecord
	var oldHash uint32

	// loop over existing records, starting from hash, trying to find one that has
	// the exact same queries as the one we're inserting
	for i := uint32(0); i < collisionAvoidanceWindow; i++ {
		if existingRecord, ok := m[hash+i]; ok {
			if record.matchesQueries(existingRecord) {
				oldRecord = &existingRecord
				oldHash = hash + i
				break
			}
		}
	}

	// now find the best position to insert the new/updated record, starting from hash
	for i := uint32(0); i < collisionAvoidanceWindow; i++ {
		// if we find a free slot, then insert the new record there
		if _, ok := m[hash]; !ok {
			// add the new record, as long as it has meta tags
			if record.hasMetaTags() {
				m[hash] = record
			}

			// if we're updating a record, then we need to delete the old entry
			if oldRecord != nil {
				delete(m, oldHash)
			}

			return hash, &record, oldHash, oldRecord, nil
		}

		// replace existing old record with the new one, at the same hash id
		if oldRecord != nil && oldHash == hash {
			if record.hasMetaTags() {
				m[hash] = record
			} else {
				// if the new record has no meta tags, then we simply delete the entry
				delete(m, hash)
			}
			return hash, &record, oldHash, oldRecord, nil
		}
		hash++
	}

	return 0, nil, 0, nil, fmt.Errorf("MetaTagRecordUpsert: Unable to find a slot to insert record")
}
