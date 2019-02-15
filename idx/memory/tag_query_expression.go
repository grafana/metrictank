package memory

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/grafana/metrictank/idx"
)

type match uint8

const (
	opNone      match = iota
	opEqual           // =
	opNotEqual        // !=
	opMatch           // =~             regular expression
	opNotMatch        // !=~
	opPrefix          // ^=             exact prefix, not regex. non-standard, required for auto complete of tag values
	opMatchTag        // __tag=~        relies on special key __tag. non-standard, required for `/metrics/tags` requests with "filter"
	opHasTag          // <tag>!=<empty> only metrics that have the tag <tag> with any value
	opNotHasTag       // <tag>=<empty>  only metrics that do not have the tag <tag> set
	opPrefixTag       // __tag^=        exact prefix with tag. non-standard, required for auto complete of tag keys
)

// every query must have at least one expression with a positive operator
var positiveOperators []match = []match{opEqual, opMatch, opPrefix, opHasTag, opPrefixTag, opMatchTag}

type filterDecision uint8

const (
	none filterDecision = iota // no decision has been made
	fail                       // it has been decided by the filter that this id does not end up in the result set
	pass                       // the filter has passed, if there are more filters remaining then call the next one
)

type tagFilter func(*idx.Archive) filterDecision
type tagStringMatcher func(string) bool

type expression interface {
	getFilter() tagFilter
	getDefaultDecision() filterDecision
	getKey() string
	getValue() string
	getOperator() match
	getMatcher() tagStringMatcher
	getMetaRecords(metaTagIndex) []uint32
	hasRe() bool
	matchesTag() bool
	isPositiveOperator() bool
	stringIntoBuilder(builder *strings.Builder)
}

// parseExpression returns an expression that's been generated from the given
// string, in case of an error the error gets returned as the second value
func parseExpression(expr string) (expression, error) {
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
			return nil, errInvalidQuery
		}
	}

	// key must not be empty
	if pos == 0 {
		return nil, errInvalidQuery
	}

	resCommon.key = expr[:pos]

	// shift over the !/^ characters
	if not || prefix {
		pos++
	}

	if len(expr) <= pos || expr[pos] != '=' {
		return nil, errInvalidQuery
	}
	pos++

	if len(expr) > pos && expr[pos] == '~' {
		// ^=~ is not a valid operator
		if prefix {
			return nil, errInvalidQuery
		}
		regex = true
		pos++
	}

	valuePos := pos
	for ; pos < len(expr); pos++ {
		// disallow ; in value
		if expr[pos] == 59 {
			return nil, errInvalidQuery
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
			return nil, errInvalidQuery
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

func (e *expressionCommon) getKey() string {
	return e.key
}

func (e *expressionCommon) getValue() string {
	return e.value
}

func (e *expressionCommon) getOperator() match {
	return e.operator
}

func (e *expressionCommon) isPositiveOperator() bool {
	for _, op := range positiveOperators {
		if e.operator == op {
			return true
		}
	}
	return false
}

func (e *expressionCommon) stringIntoBuilder(builder *strings.Builder) {
	switch e.operator {
	case opPrefixTag:
		builder.WriteString("__tag^=")
	}
	builder.WriteString(e.value)
}

func (e *expressionCommon) getMatcher() tagStringMatcher {
	// only used with positive expressions
	return nil
}

func (e *expressionCommon) getMetaRecords(mti metaTagIndex) []uint32 {
	// only used with positive expressions
	return nil
}
