package tagquery

import (
	"github.com/grafana/metrictank/errors"
	"github.com/grafana/metrictank/schema"
	"github.com/grafana/metrictank/util"
)

var (
	errInvalidQuery = errors.NewBadRequest("invalid query")
	MatchCacheSize  int
	MetaTagSupport  bool

	// the function we use to get the hash for hashing the meta records
	// it can be replaced for mocking in tests
	QueryHash func() util.StringHash32
)

func init() {
	QueryHash = util.NewFnv32aStringWriter
}

//Query is a set of graphite tag expressions as used by the graphite `seriesByTag` function.
//tag expression definitions: https://graphite.readthedocs.io/en/latest/tags.html#querying
//seriesByTag documentation: https://graphite.readthedocs.io/en/latest/functions.html#graphite.render.functions.seriesByTag
type Query struct {
	// clauses that operates on LastUpdate field
	From int64
	To   int64

	// slice of expressions sorted by the estimated cost of their operators
	Expressions Expressions

	// the index of clause that operate on tags (keys)
	// we only support 0 or 1 tag expression per query
	// tag expressions are __tag^= and __tag=~
	tagClause int
}

//NewQueryFromStrings parses a list of graphite tag expressions as used by the graphite `seriesByTag` function.
//tag expression definitions: https://graphite.readthedocs.io/en/latest/tags.html#querying
//seriesByTag documentation: https://graphite.readthedocs.io/en/latest/functions.html#graphite.render.functions.seriesByTag
//Some possible tag expressions are: "status=200", "path!=/", "name=~cpu\..*" (`name` is  a special tag which is automatically applied to the metric name).
func NewQueryFromStrings(expressionStrs []string, from int64, to int64) (Query, error) {
	var res Query
	expressions, err := ParseExpressions(expressionStrs)
	if err != nil {
		return res, err
	}
	return NewQuery(expressions, from, to)
}

func NewQuery(expressions Expressions, from int64, to int64) (Query, error) {
	q := Query{From: from, To: to, tagClause: -1}

	if len(expressions) == 0 {
		return q, errInvalidQuery
	}

	expressions.Sort()
	foundExpressionRequiringNonEmptyValue := false
	for i := 0; i < len(expressions); i++ {
		// skip duplicate expression
		if i > 0 && expressions[i].Equals(expressions[i-1]) {
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

	return q, nil
}

type IdTagLookup func(id schema.MKey, tag, value string) bool

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
