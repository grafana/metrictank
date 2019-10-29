package tagquery

import (
	"fmt"

	"github.com/grafana/metrictank/schema"
)

type MetaTagRecord struct {
	MetaTags    Tags        `json:"metaTags"`
	Expressions Expressions `json:"expressions"`
}

func ParseMetaTagRecord(metaTags []string, expressions []string) (MetaTagRecord, error) {
	res := MetaTagRecord{}
	var err error

	res.MetaTags, err = ParseTags(metaTags)
	if err != nil {
		return res, err
	}

	res.Expressions, err = ParseExpressions(expressions)
	if err != nil {
		return res, err
	}

	// we don't actually need to instantiate a query at this point, but we want to verify
	// that it is possible to instantiate a query from the given meta record expressions.
	// if we can't instantiate a query from the given expressions, then the meta record
	// upsert request should be considered invalid and should get rejected.
	_, err = NewQuery(res.Expressions, 0)
	if err != nil {
		return res, fmt.Errorf("Failed to instantiate query from given expressions: %s", err)
	}

	if len(res.Expressions) == 0 {
		return res, fmt.Errorf("Meta Tag Record must have at least one query")
	}

	return res, nil
}

// Equals takes another MetaTagRecord and compares all its properties to its
// own properties. It is assumed that the expressions of both meta tag records
// are already sorted.
func (m *MetaTagRecord) Equals(other *MetaTagRecord) bool {
	if len(m.MetaTags) != len(other.MetaTags) {
		return false
	}

	for i := range m.MetaTags {
		if m.MetaTags[i] != other.MetaTags[i] {
			return false
		}
	}

	return m.EqualExpressions(other)
}

// HashExpressions returns a hash of all expressions in this meta tag record
// It is assumed that the expressions are already sorted
func (m *MetaTagRecord) HashExpressions() uint32 {
	h := QueryHash()
	for _, expression := range m.Expressions {
		expression.StringIntoWriter(h)

		// trailing ";" doesn't matter, this is only hash input
		h.WriteString(";")
	}

	return h.Sum32()
}

// HashMetaTags returns a hash of all meta tags in this meta tag record
// It is assumed that the meta tags are already sorted
func (m *MetaTagRecord) HashMetaTags() uint32 {
	h := QueryHash()
	for _, metaTag := range m.MetaTags {
		metaTag.StringIntoWriter(h)

		// trailing ";" doesn't matter, this is only hash input
		h.WriteString(";")
	}

	return h.Sum32()
}

func (m *MetaTagRecord) HashRecord() uint64 {
	expressionsHash := m.HashExpressions()
	metaTagHash := m.HashMetaTags()

	return uint64(expressionsHash) | uint64(metaTagHash)<<32
}

// EqualExpressions compares another meta tag record's expressions to
// this one's expressions
// Returns true if they are equal, otherwise false
// It is assumed that all the expressions are already sorted
func (m *MetaTagRecord) EqualExpressions(other *MetaTagRecord) bool {
	if len(m.Expressions) != len(other.Expressions) {
		return false
	}

	for i, expression := range m.Expressions {
		if !expression.Equals(other.Expressions[i]) {
			return false
		}
	}

	return true
}

// HasMetaTags returns true if the meta tag record has one or more
// meta tags, otherwise it returns false
func (m *MetaTagRecord) HasMetaTags() bool {
	return len(m.MetaTags) > 0
}

func (m *MetaTagRecord) GetMetricDefinitionFilter(lookup IdTagLookup) MetricDefinitionFilter {
	filters := make([]MetricDefinitionFilter, len(m.Expressions))
	defaultDecisions := make([]FilterDecision, len(m.Expressions))
	for i, expr := range m.Expressions {
		filters[i] = expr.GetMetricDefinitionFilter(lookup)
		defaultDecisions[i] = expr.GetDefaultDecision()
	}

	return func(id schema.MKey, name string, tags []string) FilterDecision {
		for i := range filters {
			decision := filters[i](id, name, tags)

			if decision == None {
				return defaultDecisions[i]
			}

			if decision == Pass {
				continue
			}

			if decision == Fail {
				return Fail
			}
		}

		return Pass
	}
}
