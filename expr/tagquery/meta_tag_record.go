package tagquery

import (
	"fmt"
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

func (m *MetaTagRecord) Equals(other *MetaTagRecord) bool {
	if len(m.MetaTags) != len(other.MetaTags) {
		return false
	}

	foundTags := make([]bool, len(m.MetaTags))
	for i, tag := range m.MetaTags {
		for _, otherTag := range other.MetaTags {
			if tag == otherTag {
				foundTags[i] = true
			}
		}
	}

	for i := range foundTags {
		if !foundTags[i] {
			return false
		}
	}

	return m.EqualExpressions(other)

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
