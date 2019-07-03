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

	// clauses that operate on values. from expressions like tag<operator>value
	Expressions map[ExpressionOperator]Expressions

	// clause that operate on tags (keys)
	// we only need to support 1 condition for now: a prefix or match
	TagClause ExpressionOperator // to know the clause type. either PREFIX_TAG or MATCH_TAG (or 0 if unset)
	TagMatch  Expression         // only used for /metrics/tags with regex in filter param
	TagPrefix string             // only used for auto complete of tags to match exact prefix

	StartWith ExpressionOperator // choses the first clause to generate the initial result set (one of EQUAL PREFIX MATCH MATCH_TAG PREFIX_TAG)
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

	expressions.Sort()
	q.Expressions = make(map[ExpressionOperator]Expressions)
	for i, e := range expressions {
		// skip duplicate expression
		if i > 0 && e.IsEqualTo(expressions[i-1]) {
			continue
		}

		// special case of empty value
		if len(e.Value) == 0 {
			if e.Operator == EQUAL || e.Operator == MATCH {
				q.Expressions[NOT_MATCH] = append(q.Expressions[NOT_MATCH], e)
			} else {
				q.Expressions[MATCH] = append(q.Expressions[MATCH], e)
			}
		} else {
			switch e.Operator {
			case EQUAL:
				q.Expressions[EQUAL] = append(q.Expressions[EQUAL], e)
			case NOT_EQUAL:
				q.Expressions[NOT_EQUAL] = append(q.Expressions[NOT_EQUAL], e)
			case MATCH:
				q.Expressions[MATCH] = append(q.Expressions[MATCH], e)
			case NOT_MATCH:
				q.Expressions[NOT_MATCH] = append(q.Expressions[NOT_MATCH], e)
			case PREFIX:
				q.Expressions[PREFIX] = append(q.Expressions[PREFIX], e)
			case MATCH_TAG:
				// we only allow one expression operating on tags
				if q.TagClause != 0 {
					return q, errInvalidQuery
				}

				q.TagMatch = e
				q.TagClause = MATCH_TAG
			case PREFIX_TAG:
				// we only allow one expression operating on tags
				if q.TagClause != 0 {
					return q, errInvalidQuery
				}

				q.TagPrefix = e.Value
				q.TagClause = PREFIX_TAG
			}
		}
	}

	// the cheapest operator to minimize the result set should have precedence
	if len(q.Expressions[EQUAL]) > 0 {
		q.StartWith = EQUAL
	} else if len(q.Expressions[PREFIX]) > 0 {
		q.StartWith = PREFIX
	} else if len(q.Expressions[MATCH]) > 0 {
		q.StartWith = MATCH
	} else if q.TagClause == PREFIX_TAG {
		// starting with a tag based expression can be very expensive because they
		// have the potential to result in a huge initial result set
		q.StartWith = PREFIX_TAG
	} else if q.TagClause == MATCH_TAG {
		q.StartWith = MATCH_TAG
	} else {
		return q, errInvalidQuery
	}

	return q, nil
}
