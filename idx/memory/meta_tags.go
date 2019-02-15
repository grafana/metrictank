package memory

import (
	"fmt"
	"github.com/grafana/metrictank/idx/memory/tagQueryExpression"
	"hash"
	"hash/fnv"
	"sort"
	"strings"

	"github.com/grafana/metrictank/errors"
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
type metaTagRecords map[recordId]metaTagRecord

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
func (m metaTagRecords) upsert(metaTags []string, tagQueryExpressions []string) (recordId, *metaTagRecord, recordId, *metaTagRecord, error) {
	record, err := newMetaTagRecord(metaTags, tagQueryExpressions)
	if err != nil {
		return 0, nil, 0, nil, err
	}

	record.sortQueries()
	id := record.hashQueries()
	var oldRecord *metaTagRecord
	var oldId recordId

	// loop over existing records, starting from id, trying to find one that has
	// the exact same queries as the one we're upserting
	for i := uint32(0); i < collisionAvoidanceWindow; i++ {
		if existingRecord, ok := m[id+recordId(i)]; ok {
			if record.matchesQueries(existingRecord) {
				oldRecord = &existingRecord
				oldId = id + recordId(i)
				delete(m, oldId)
				break
			}
		}
	}

	if !record.hasMetaTags() {
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

func (m metaTagRecords) getRecords(ids []recordId) []metaTagRecord {
	res := make([]metaTagRecord, 0, len(ids))

	for _, id := range ids {
		if record, ok := m[id]; ok {
			res = append(res, record)
		}
	}

	return res
}

func (m metaTagRecords) enrichTags(tags map[string]string) map[string]string {
	res := make(map[string]string)
	for _, mtr := range m {
		if mtr.matchesTags(tags) {
			for _, kv := range mtr.metaTags {
				res[kv.key] = kv.value
			}
		}
	}

	return res
}

type metaTagRecord struct {
	metaTags    []kv
	queries     []tagQueryExpression.Expression
	matchesTags metaRecordFilter
}

type metaRecordFilter func(map[string]string) bool

// newMetaTagRecord takes two slices of strings, parses them and returns a metaTagRecord
// The first slice of strings are the meta tags & values
// The second slice is the tag query expressions which the meta key & values refer to
// On parsing error the second returned value is an error, otherwise it is nil
func newMetaTagRecord(metaTags []string, tagQueryExpressions []string) (metaTagRecord, error) {
	record := metaTagRecord{
		metaTags: make([]kv, 0, len(metaTags)),
		queries:  make([]tagQueryExpression.Expression, 0, len(tagQueryExpressions)),
	}
	if len(tagQueryExpressions) == 0 {
		return record, errors.NewBadRequest("Requiring at least one tag query expression, 0 given")
	}

	for _, tag := range metaTags {
		tagSplits := strings.SplitN(tag, "=", 2)
		if len(tagSplits) < 2 {
			return record, errors.NewBadRequest(fmt.Sprintf("Missing \"=\" sign in tag %s", tag))
		}
		if len(tagSplits[0]) == 0 || len(tagSplits[1]) == 0 {
			return record, errors.NewBadRequest(fmt.Sprintf("Tag/Value cannot be empty in %s", tag))
		}

		record.metaTags = append(record.metaTags, kv{key: tagSplits[0], value: tagSplits[1]})
	}

	haveTagOperator := false
	for _, query := range tagQueryExpressions {
		parsed, err := tagQueryExpression.ParseExpression(query)
		if err != nil {
			return record, err
		}
		if parsed.IsTagOperator() {
			if haveTagOperator {
				return record, fmt.Errorf("Only one tag operator is allowed per query")
			}
			haveTagOperator = true
		}
		record.queries = append(record.queries, parsed)
	}

	record.buildMetaRecordFilter()

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
		query.StringIntoBuilder(builder)
		res[i] = builder.String()
		builder.Reset()
	}

	return res
}

// hashQueries generates a hash of all the queries in the record
func (m *metaTagRecord) hashQueries() recordId {
	builder := strings.Builder{}
	for _, query := range m.queries {
		query.StringIntoBuilder(&builder)

		// trailing ";" doesn't matter, this is only hash input
		builder.WriteString(";")
	}

	h := queryHash()
	h.Write([]byte(builder.String()))
	return recordId(h.Sum32())
}

// sortQueries sorts all the queries first by key, then by value, then by
// operator. The order itself is not relevant, we only need to guarantee
// that the order is consistent for a given set of queries after every call
// to this function, because the queries will then be used as hash input
func (m *metaTagRecord) sortQueries() {
	sort.Slice(m.queries, func(i, j int) bool {
		if m.queries[i].GetKey() == m.queries[j].GetKey() {
			if m.queries[i].GetValue() == m.queries[j].GetValue() {
				return m.queries[i].GetOperator() < m.queries[j].GetOperator()
			}
			return m.queries[i].GetValue() < m.queries[j].GetValue()
		}
		return m.queries[i].GetKey() < m.queries[j].GetKey()
	})
}

// matchesQueries compares another tag record's queries to this
// one's queries. Returns true if they are equal, otherwise false.
// It is assumed that all the queries are already sorted
func (m *metaTagRecord) matchesQueries(other metaTagRecord) bool {
	if len(m.queries) != len(other.queries) {
		return false
	}

	for id, query := range m.queries {
		if query.GetKey() != other.queries[id].GetKey() {
			return false
		}

		if query.GetOperator() != other.queries[id].GetOperator() {
			return false
		}

		if query.GetValue() != other.queries[id].GetValue() {
			return false
		}
	}

	return true
}

// hasMetaTags returns true if the meta tag record has one or more
// meta tags, otherwise it returns false
func (m *metaTagRecord) hasMetaTags() bool {
	return len(m.metaTags) > 0
}

// buildMetaRecordFilter builds a function to which a set of tags&values can be passed,
// it then evaluate whether the queries of this meta tag record all match with the given tags
// this is used for the series enrichment
func (m *metaTagRecord) buildMetaRecordFilter() {
	queries := make([]tagQueryExpression.Expression, len(m.queries))
	copy(queries, m.queries)

	// we want to sort the queries so the regexes come last, because they are more expensive
	sort.Slice(queries, func(i, j int) bool {
		iHasRe := queries[i].HasRe()
		jHasRe := queries[j].HasRe()

		// when both have a regex or both have no regex, they are considered equal
		if iHasRe == jHasRe {
			return false
		}

		// if i has no regex, but j has one, i is considered cheaper
		if iHasRe == false {
			return true
		}

		// if j has no regex, but i has one, then j is considered cheaper
		return false
	})

	// generate all the filter functions for each of the queries
	filters := make([]func(string, string) bool, 0, len(queries))
	for _, query := range queries {
		matcher := query.GetMatcher()
		if query.MatchesTag() {
			filters = append(filters, func(tag, value string) bool {
				return matcher(tag)
			})
			continue
		}
		queryKey := query.GetKey()
		filters = append(filters, func(tag, value string) bool {
			if tag != queryKey {
				return false
			}
			return matcher(value)
		})
	}

	// generate one function which applies all filters to the given set of tags & values
	// it returns true if the given tags satisfy the meta records query conditions
	m.matchesTags = func(tags map[string]string) bool {
	FILTERS:
		for _, filter := range filters {
			for tag, value := range tags {
				if filter(tag, value) {
					// if one tag/value satisfies the filter we can move on to the next filer
					continue FILTERS
				}
			}

			// if we checked all tag/value pairs, but none satisfied the filter, return false
			return false
		}
		return true
	}
}

// index structure keyed by tag -> value -> list of meta record IDs
type metaTagValue map[string][]recordId
type metaTagIndex map[string]metaTagValue

func (m metaTagIndex) deleteRecord(keyValue kv, id recordId) {
	if values, ok := m[keyValue.key]; ok {
		if ids, ok := values[keyValue.value]; ok {
			for i := 0; i < len(ids); i++ {
				if ids[i] == id {
					// no need to keep the order
					ids[i] = ids[len(ids)-1]
					values[keyValue.value] = ids[:len(ids)-1]

					// no id should ever be present more than once
					return
				}
			}
		}
	}
}

func (m metaTagIndex) insertRecord(keyValue kv, id recordId) {
	var values metaTagValue
	var ok bool

	if values, ok = m[keyValue.key]; !ok {
		values = make(metaTagValue)
		m[keyValue.key] = values
	}

	values[keyValue.value] = append(values[keyValue.value], id)
}

// getMetaRecordIdsByExpression takes an expression and returns all meta record
// ids of the records which match it.
// It is important to note that negative expressions get evaluated as their
// positive equivalent, f.e. != gets evaluated as =, to reduce the size of the
// result set. The caller will then need to handle the negation according to the
// expression type.
func (m metaTagIndex) getMetaRecordIdsByExpression(expression tagQueryExpression.Expression) []recordId {
	resultMap := make(map[recordId]struct{})
	if expression.IsTagOperator() {
		matcher := expression.GetMatcher()
		for tag, recordIdsByValue := range m {
			if matcher(tag) {
				for _, recordIds := range recordIdsByValue {
					for _, recordId := range recordIds {
						resultMap[recordId] = struct{}{}
					}
				}
			}
		}
	} else {
		if values, ok := m[expression.GetKey()]; ok {
			matcher := expression.GetMatcher()
			for value, recordIdsByValue := range values {
				if matcher(value) {
					for _, recordId := range recordIdsByValue {
						resultMap[recordId] = struct{}{}
					}
				}
			}
		}
	}

	res := make([]recordId, 0, len(resultMap))
	for id := range resultMap {
		res = append(res, id)
	}
	return res
}
