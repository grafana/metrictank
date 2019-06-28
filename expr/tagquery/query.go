package tagquery

import (
	"errors"
)

var (
	errInvalidQuery = errors.New("invalid query")
)

type Query struct {
	// clause that operates on LastUpdate field
	From int64

	// slice of expressions sorted by the estimated cost of their operators
	Expressions Expressions

	// the index in the Expressions slice at which we start evaluating the query
	StartWith int

	// clause that operate on tags (keys)
	// we only need to support 1 condition for now: a prefix or match
	TagClause Expression
}

func NewQueryFromStrings(expressionStrs []string, from int64) (Query, error) {
	var res Query
	expressions, err := ParseExpressions(expressionStrs)
	if err != nil {
		return res, err
	}
	return NewQuery(expressions, from)
}

func NewQuery(expressions Expressions, from int64) (Query, error) {
	q := Query{From: from}

	if len(expressions) == 0 {
		return q, errInvalidQuery
	}

	expressions.SortByFilterOrder()
	for i := 0; i < len(expressions); i++ {
		// skip duplicate expression
		if i > 0 && ExpressionsAreEqual(expressions[i], expressions[i-1]) {
			expressions = append(expressions[:i], expressions[i+1:]...)
			i--
			continue
		}

		op := expressions[i].GetOperator()
		switch op {
		case MATCH_TAG:
			fallthrough
		case PREFIX_TAG:
			// we only allow one query by tag
			if q.TagClause != nil {
				return q, errInvalidQuery
			}

			q.TagClause = expressions[i]
			expressions = append(expressions[:i], expressions[i+1:]...)
			i--
		}
	}

	q.Expressions = expressions
	q.StartWith = q.Expressions.findInitialExpression()
	if q.TagClause == nil && q.StartWith < 0 {
		return q, errInvalidQuery
	}

	return q, nil
}

func (q *Query) GetMetricDefinitionFilters() (MetricDefinitionFilters, []FilterDecision) {
	var filters MetricDefinitionFilters
	var defaultDecisions []FilterDecision
	for i := range q.Expressions {
		// the one we start with does not need to be added to the filters,
		// because we use it to build the initial result set
		if i == q.StartWith {
			continue
		}
		filters = append(filters, q.Expressions[i].GetMetricDefinitionFilter())
		defaultDecisions = append(defaultDecisions, q.Expressions[i].GetDefaultDecision())
	}

	return filters, defaultDecisions
}
