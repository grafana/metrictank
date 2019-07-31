package tagquery

import (
	"fmt"
	"strings"

	"github.com/raintank/schema"
)

type MetaTagRecord struct {
	MetaTags    Tags
	Expressions Expressions
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
	builder := strings.Builder{}
	for _, query := range m.Expressions {
		query.StringIntoBuilder(&builder)

		// trailing ";" doesn't matter, this is only hash input
		builder.WriteString(";")
	}

	h := QueryHash()
	h.Write([]byte(builder.String()))
	return h.Sum32()
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
