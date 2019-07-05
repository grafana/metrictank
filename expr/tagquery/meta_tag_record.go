package tagquery

import (
	"fmt"
)

type MetaTagRecord struct {
	MetaTags    Tags
	Expressions Expressions
}

func ParseMetaTagRecord(metaTags []string, queries []string) (MetaTagRecord, error) {
	res := MetaTagRecord{}
	var err error

	res.MetaTags, err = ParseTags(metaTags)
	if err != nil {
		return res, err
	}

	res.Expressions, err = ParseExpressions(queries)
	if err != nil {
		return res, err
	}

	if len(res.Expressions) == 0 {
		return res, fmt.Errorf("Meta Tag Record must have at least one query")
	}

	return res, nil
}

// MatchesQueries compares another tag record's queries to this
// one's queries. Returns true if they are equal, otherwise false.
// It is assumed that all the queries are already sorted
func (m *MetaTagRecord) MatchesQueries(other *MetaTagRecord) bool {
	if len(m.Expressions) != len(other.Expressions) {
		return false
	}

	for i, query := range m.Expressions {
		if !ExpressionsAreEqual(query, other.Expressions[i]) {
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
