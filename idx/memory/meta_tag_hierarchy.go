package memory

import (
	"regexp"
	"strings"
	"sync"

	"github.com/grafana/metrictank/expr/tagquery"
)

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
