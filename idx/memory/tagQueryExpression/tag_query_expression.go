package tagQueryExpression

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/grafana/metrictank/idx"
)

type match uint8

const (
	// = equal tag & value
	opEqual match = iota + 1

	// != unequal tag & value
	opNotEqual

	// =~ regular expression match of a tag value
	opMatch

	// !=~ regular expression non-match of a tag value
	opNotMatch

	// ^= prefix match of a tag value
	// example:
	// the expression "key1^=val" evaluates true for "key1=value1"
	opPrefix

	// __tag=~ regular expression match on the tag
	// example:
	// the expression "__tag=~key?" evaluates to true for "key1=value1"
	opMatchTag

	// <tag>!= will evaluate to true if the tag <tag> is present,
	// the tag's value doesn't matter
	// example:
	// the expression "key1!=" evaluates to true for "key1=value1"
	opHasTag

	opNotHasTag
	opPrefixTag
)

type FilterDecision uint8

const (
	None FilterDecision = iota // no decision has been made, because the decision might change depending on what other indexes define
	Fail                       // it has been decided by the filter that this id does not end up in the result set
	Pass                       // the filter has passed, if there are more filters remaining then call the next one
)

var matchCacheSize int

// TagSetFilter takes a set of tags and returns a FilterDecision
// the FilterDecision can be pass, fail, or none
type TagSetFilter func(map[string]string) FilterDecision

// MetricDefinitionFilter takes a metric definition and returns a FilterDecision
// similar like tagSetFilter, but with different optimizations
type MetricDefinitionFilter func(*idx.Archive) FilterDecision

type TagStringMatcher func(string) bool

type Expression interface {
	// GetMetricDefinitionFilter() returns a MetricDefinitionFilter, that is
	// the same like a TagSetFilter, but it operates on a MetricDefinition to
	// allow for additional optimizations
	GetMetricDefinitionFilter() MetricDefinitionFilter

	// GetDefaultDecision defines what decision should be made if the filter only returned "none"
	// this is necessary in cases where a filter cannot come to a conclusive decision based on one index
	// example:
	// when evaluating the expression key1!= based on the tags associated with a metric definition we can
	// be sure that the condition will "pass" if there is a tags with they key "key1", but if there is
	// no tag with the key "key1" it is still possible that such a tag will get added via the meta index
	GetDefaultDecision() FilterDecision

	// the tag to which this expression get's applied
	// example:
	// in the expression "tag1=value" getKey() would return "tag1"
	GetKey() string
	GetValue() string
	GetOperator() match
	GetMatcher() TagStringMatcher
	HasRe() bool
	MatchesTag() bool
	IsPositiveOperator() bool
	IsTagOperator() bool
	StringIntoBuilder(builder *strings.Builder)
}

const invalidExpressionError = "Invalid expression: %s"

// ParseExpression returns an expression that's been generated from the given
// string, in case of an error the error gets returned as the second value
func ParseExpression(expr string) (Expression, error) {
	var pos int
	prefix, regex, not := false, false, false
	resCommon := expressionCommon{}

	// scan up to operator to get key
FIND_OPERATOR:
	for ; pos < len(expr); pos++ {
		switch expr[pos] {
		case '=':
			break FIND_OPERATOR
		case '!':
			not = true
			break FIND_OPERATOR
		case '^':
			prefix = true
			break FIND_OPERATOR
		case ';':
			return nil, fmt.Errorf(invalidExpressionError, expr)
		}
	}

	// key must not be empty
	if pos == 0 {
		return nil, fmt.Errorf(invalidExpressionError, expr)
	}

	resCommon.key = expr[:pos]

	// shift over the !/^ characters
	if not || prefix {
		pos++
	}

	if len(expr) <= pos || expr[pos] != '=' {
		return nil, fmt.Errorf(invalidExpressionError, expr)
	}
	pos++

	if len(expr) > pos && expr[pos] == '~' {
		// ^=~ is not a valid operator
		if prefix {
			return nil, fmt.Errorf(invalidExpressionError, expr)
		}
		regex = true
		pos++
	}

	valuePos := pos
	for ; pos < len(expr); pos++ {
		// disallow ; in value
		if expr[pos] == 59 {
			return nil, fmt.Errorf(invalidExpressionError, expr)
		}
	}
	resCommon.value = expr[valuePos:]

	if not {
		if len(resCommon.value) == 0 {
			resCommon.operator = opHasTag
		} else if regex {
			resCommon.operator = opNotMatch
		} else {
			resCommon.operator = opNotEqual
		}
	} else {
		if prefix {
			if len(resCommon.value) == 0 {
				resCommon.operator = opHasTag
			} else {
				resCommon.operator = opPrefix
			}
		} else if len(resCommon.value) == 0 {
			resCommon.operator = opNotHasTag
		} else if regex {
			resCommon.operator = opMatch
		} else {
			resCommon.operator = opEqual
		}
	}

	// special key to match on tag instead of a value
	if resCommon.key == "__tag" {
		// currently ! (not) queries on tags are not supported
		// and unlike normal queries a value must be set
		if not || len(resCommon.value) == 0 {
			return nil, fmt.Errorf(invalidExpressionError, expr)
		}

		if resCommon.operator == opPrefix {
			resCommon.operator = opPrefixTag
		} else if resCommon.operator == opMatch {
			resCommon.operator = opMatchTag
		}
	}

	if resCommon.operator == opMatch || resCommon.operator == opNotMatch || resCommon.operator == opMatchTag {
		if len(resCommon.value) > 0 && resCommon.value[0] != '^' {
			resCommon.value = "^(?:" + resCommon.value + ")"
		}

		valueRe, err := regexp.Compile(resCommon.value)
		if err != nil {
			return nil, err
		}
		switch resCommon.operator {
		case opMatch:
			return &expressionMatch{expressionCommon: resCommon, valueRe: valueRe}, nil
		case opNotMatch:
			return &expressionNotMatch{expressionCommon: resCommon, valueRe: valueRe}, nil
		case opMatchTag:
			return &expressionMatchTag{expressionCommon: resCommon, valueRe: valueRe}, nil
		}
	} else {
		switch resCommon.operator {
		case opEqual:
			return &expressionEqual{expressionCommon: resCommon}, nil
		case opNotEqual:
			return &expressionNotEqual{expressionCommon: resCommon}, nil
		case opPrefix:
			return &expressionPrefix{expressionCommon: resCommon}, nil
		case opMatchTag:
			return &expressionMatchTag{expressionCommon: resCommon}, nil
		case opHasTag:
			return &expressionHasTag{expressionCommon: resCommon}, nil
		case opNotHasTag:
			return &expressionNotHasTag{expressionCommon: resCommon}, nil
		case opPrefixTag:
			return &expressionPrefixTag{expressionCommon: resCommon}, nil
		}
	}

	return nil, fmt.Errorf("ParseExpression: Invalid operator in expression %s", expr)
}

type expressionCommon struct {
	key      string
	value    string
	operator match
}

func (e *expressionCommon) GetKey() string {
	return e.key
}

func (e *expressionCommon) GetValue() string {
	return e.value
}

func (e *expressionCommon) GetOperator() match {
	return e.operator
}

// by default assume true, unless a concrete type overrides this method
func (e *expressionCommon) IsPositiveOperator() bool {
	return true
}

// by default assume false, unless a concrete type overrides this method
func (e *expressionCommon) IsTagOperator() bool {
	return false
}

func (e *expressionCommon) GetMatcher() TagStringMatcher {
	return nil
}
