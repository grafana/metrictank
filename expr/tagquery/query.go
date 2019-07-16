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
	startWith int

	// the index of clause that operate on tags (keys)
	// we only support 0 or 1 tag expression per query
	// tag expressions are __tag^= and __tag=~
	tagClause int
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
	q := Query{From: from, tagClause: -1}

	if len(expressions) == 0 {
		return q, errInvalidQuery
	}

	foundExpressionRequiringNonEmptyValue := false
	expressions.SortByFilterOrder()
	for i := 0; i < len(expressions); i++ {
		// skip duplicate expression
		if i > 0 && ExpressionsAreEqual(expressions[i], expressions[i-1]) {
			expressions = append(expressions[:i], expressions[i+1:]...)
			i--
			continue
		}

		foundExpressionRequiringNonEmptyValue = foundExpressionRequiringNonEmptyValue || expressions[i].RequiresNonEmptyValue()

		op := expressions[i].GetOperator()
		switch op {
		case MATCH_TAG:
			fallthrough
		case PREFIX_TAG:
			// we only allow one expression operating on the tag per query
			if q.tagClause >= 0 {
				return q, errInvalidQuery
			}

			q.tagClause = i
		}
	}

	if !foundExpressionRequiringNonEmptyValue {
		return q, errInvalidQuery
	}

	q.Expressions = expressions
	q.startWith = q.Expressions.findInitialExpression()
	if q.startWith < 0 {
		return q, errInvalidQuery
	}

	return q, nil
}

// GetMetricDefinitionFilters returns all the metric definition filters associated with this
// query, together with their according default decision
// The returned filters get generated from the query expressions, excluding the one which has
// been dedicated to be the initial expression (marked via the .startWith index)
func (q *Query) GetMetricDefinitionFilters() ([]MetricDefinitionFilter, []FilterDecision) {
	var filters []MetricDefinitionFilter
	var defaultDecisions []FilterDecision
	for i := range q.Expressions {
		// the one we start with does not need to be added to the filters,
		// because we use it to build the initial result set
		if i == q.startWith {
			continue
		}
		filters = append(filters, q.Expressions[i].GetMetricDefinitionFilter())
		defaultDecisions = append(defaultDecisions, q.Expressions[i].GetDefaultDecision())
	}

	return filters, defaultDecisions
}

// GetInitialExpression returns the expression which should be used to generate the initial
// result set, to later filter it down with the remaining expressions.
// We assume Query has been instantiated via NewQuery(), in which case it is guaranteed that
// that .startWith has been set correctly or otherwise an error would have been returned
func (q *Query) GetInitialExpression() Expression {
	return q.Expressions[q.startWith]
}

// GetTagClause returns the expression which operates on tags, if one is present.
// This assumes that Query has been instantiated via NewQuery(), which either sets
// .tagClause to a valid value or returns an error.
// There can only be one tagClause per Query.
func (q *Query) GetTagClause() Expression {
	if q.tagClause < 0 {
		return nil
	}
	return q.Expressions[q.tagClause]
}
