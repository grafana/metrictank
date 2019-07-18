package tagquery

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/raintank/schema"
)

const invalidExpressionError = "Invalid expression: %s"

type Expressions []Expression

func ParseExpressions(expressions []string) (Expressions, error) {
	res := make(Expressions, len(expressions))
	for i := range expressions {
		expression, err := ParseExpression(expressions[i])
		if err != nil {
			return nil, err
		}
		res[i] = expression
	}
	return res, nil
}

func (e Expressions) Strings() []string {
	builder := strings.Builder{}
	res := make([]string, len(e))
	for i := range e {
		e[i].StringIntoBuilder(&builder)
		res[i] = builder.String()
		builder.Reset()
	}
	return res
}

type Expression interface {
	// GetDefaultDecision defines what decision should be made if the filter has not come to a conclusive
	// decision based on a single index. When looking at more than one tag index in order of decreasing
	// priority to decide whether a metric should be part of the final result set, some operators and metric
	// combinations can come to a conclusive decision without looking at all indexes and some others can't.
	// if an expression has evaluated a metric against all indexes and has not come to a conclusive
	// decision, then the default decision gets applied.
	//
	// Example
	// metric1 has tags ["name=a.b.c", "some=value"] in the metric tag index, we evaluate the expression
	// "anothertag!=value":
	// 1) expression looks at the metric tag index and it sees that metric1 does not have a tag "anothertag"
	//    with the value "value", but at this point it doesn't know if another index that will be looked
	//    at later does, so it returns the decision "none".
	// 2) expression now looks at index2 and sees again that metric1 does not have the tag and value
	//    it is looking for, so it returns "none" again.
	// 3) the expression execution sees that there are no more indexes left, so it applies the default
	//    decision for the operator != which is "pass", meaning the expression "anothertag!=value" has
	//    not filtered the metric metric1 out of the result set.
	//
	// metric2 has tags ["name=a.b.c", "anothertag=value"] according to the metric tag index and it has
	// no meta tags, we still evaluate the same expression:
	// 1) expression looks at metric tag index and see metric2 has tag "anothertag" with value "value".
	//    it directly comes to a conclusive decision that this metric needs to be filtered out of the
	//    result set and returns the filter decision "fail".
	//
	// metric3 has tags ["name=aaa", "abc=cba"] according to the metric tag index and there is a meta
	// record assigning the tag "anothertag=value" to metrics matching that query expression "abc=cba".
	// 1) expression looks at metric3 and sees it does not have the tag & value it's looking for, so
	//    it returns the filter decision "none" because it cannot know for sure whether another index
	//    will assign "anothertag=value" to metric3.
	// 2) expression looks at the meta tag index and it sees that there are meta records matching the
	//    tag "anothertag" and the value "value", so it retrieves the according filter functions of
	//    of these meta records and passes metric3's tag set into them.
	// 3) the filter function of the meta record for the query set "abc=cba" returns true, indicating
	//    that its meta tag gets applied to metric3.
	// 4) based on that the tag expression comes to the decision that metric3 should not be part of
	//    final result set, so it returns "fail".
	GetDefaultDecision() FilterDecision

	// GetKey returns tag to who's values this expression get's applied if it operates on the value
	// (OperatorsOnTag returns "false")
	// example:
	// in the expression "tag1=value" GetKey() would return "tag1" and OperatesOnTag() returns "false"
	GetKey() string

	// GetValue the value part of the expression
	// example:
	// in the expression "abc!=cba" this would return "cba"
	GetValue() string

	// GetOperator returns the operator of this expression
	GetOperator() ExpressionOperator

	GetCostMultiplier() uint32

	// HasRe indicates whether the evaluation of this expression involves regular expressions
	HasRe() bool

	// OperatesOnTag returns true if this expression operators on the tag keys,
	// or false if it operates on the values
	OperatesOnTag() bool

	// RequiresNonEmptyValue returns boolean indicating whether this expression requires a non-empty
	// value. Every query must have at least one expression requiring a non-empty value, otherwise
	// the query is considered invalid
	RequiresNonEmptyValue() bool

	// ValuePasses takes a string which should either be a tag key or value depending on the return
	// value of OperatesOnTag(), then it returns a bool to indicate whether the given value satisfies
	// this expression
	ValuePasses(string) bool

	ValueMatchesExactly() bool

	// GetMetricDefinitionFilter returns a MetricDefinitionFilter
	// The MetricDefinitionFilter takes a metric definition, looks at its tags and returns a decision
	// regarding this query expression applied to its tags
	GetMetricDefinitionFilter(lookup IdTagLookup) MetricDefinitionFilter

	// StringIntoBuilder takes a builder and writes a string representation of this expression into it
	StringIntoBuilder(builder *strings.Builder)
}

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
	err := validateQueryExpressionTagKey(resCommon.key)
	if err != nil {
		return nil, fmt.Errorf("Error when validating key \"%s\" of expression \"%s\": %s", resCommon.key, expr, err)
	}

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
	var originalOperator, effectiveOperator ExpressionOperator

	// decide what operator this expression uses, based on the operator
	// itself, but ignoring other factors like f.e. an empty value
	if not {
		if regex {
			originalOperator = NOT_MATCH
		} else {
			originalOperator = NOT_EQUAL
		}
	} else {
		if prefix {
			originalOperator = PREFIX
		} else if regex {
			originalOperator = MATCH
		} else {
			originalOperator = EQUAL
		}
	}

	effectiveOperator = originalOperator

	// special key to match on tag instead of a value
	// update the operator decision accordingly
	if resCommon.key == "__tag" {
		// currently ! (not) queries on tags are not supported
		// and unlike normal queries a value must be set
		if not {
			return nil, fmt.Errorf(invalidExpressionError, expr)
		}

		switch effectiveOperator {
		case PREFIX:
			if len(resCommon.value) == 0 {
				effectiveOperator = MATCH_ALL
			} else {
				effectiveOperator = PREFIX_TAG
			}
		case MATCH:
			if len(resCommon.value) == 0 {
				effectiveOperator = MATCH_ALL
			} else {
				effectiveOperator = MATCH_TAG
			}
		case EQUAL:
			if len(resCommon.value) == 0 {
				return nil, fmt.Errorf(invalidExpressionError, expr)
			}

			// "__tag=abc", should internatlly be translated into "abc!="
			resCommon.key = resCommon.value
			resCommon.value = ""
			effectiveOperator = HAS_TAG
		}
	}

	// check for special case of an empty value and
	// update chosen operator accordingly
	if len(resCommon.value) == 0 {
		switch effectiveOperator {
		case EQUAL:
			effectiveOperator = NOT_HAS_TAG
		case NOT_EQUAL:
			effectiveOperator = HAS_TAG
		case MATCH:
			effectiveOperator = MATCH_ALL
		case NOT_MATCH:
			effectiveOperator = MATCH_NONE
		case PREFIX:
			effectiveOperator = MATCH_ALL
		}
	}

	if effectiveOperator == MATCH || effectiveOperator == MATCH_TAG || effectiveOperator == NOT_MATCH {
		if len(resCommon.value) > 0 && resCommon.value[0] != '^' {
			resCommon.value = "^(?:" + resCommon.value + ")"
		}

		// no need to run regular expressions that match any string
		// so we update the operator to MATCH_ALL/NONE
		if resCommon.value == "^(?:.*)" || resCommon.value == "^.*" || resCommon.value == "^(.*)" {
			switch effectiveOperator {
			case MATCH:
				return &expressionMatchAll{expressionCommon: resCommon, originalOperator: originalOperator}, nil
			case MATCH_TAG:
				return &expressionMatchAll{expressionCommon: resCommon, originalOperator: originalOperator}, nil
			case NOT_MATCH:
				return &expressionMatchNone{expressionCommon: resCommon, originalOperator: originalOperator}, nil
			}
		}

		valueRe, err := regexp.Compile(resCommon.value)
		if err != nil {
			return nil, err
		}

		// check for special case when regular expression matches
		// empty value and update operator accordingly
		matchesEmpty := valueRe.MatchString("")

		switch effectiveOperator {
		case MATCH:
			return &expressionMatch{expressionCommonRe: expressionCommonRe{expressionCommon: resCommon, valueRe: valueRe, matchesEmpty: matchesEmpty}}, nil
		case NOT_MATCH:
			return &expressionNotMatch{expressionCommonRe: expressionCommonRe{expressionCommon: resCommon, valueRe: valueRe, matchesEmpty: matchesEmpty}}, nil
		case MATCH_TAG:
			if matchesEmpty {
				return nil, fmt.Errorf(invalidExpressionError, expr)
			}
			return &expressionMatchTag{expressionCommonRe: expressionCommonRe{expressionCommon: resCommon, valueRe: valueRe, matchesEmpty: matchesEmpty}}, nil
		}
	} else {
		switch effectiveOperator {
		case EQUAL:
			return &expressionEqual{expressionCommon: resCommon}, nil
		case NOT_EQUAL:
			return &expressionNotEqual{expressionCommon: resCommon}, nil
		case PREFIX:
			return &expressionPrefix{expressionCommon: resCommon}, nil
		case HAS_TAG:
			return &expressionHasTag{expressionCommon: resCommon}, nil
		case NOT_HAS_TAG:
			return &expressionNotHasTag{expressionCommon: resCommon}, nil
		case PREFIX_TAG:
			return &expressionPrefixTag{expressionCommon: resCommon}, nil
		case MATCH_ALL:
			return &expressionMatchAll{expressionCommon: resCommon, originalOperator: originalOperator}, nil
		case MATCH_NONE:
			return &expressionMatchNone{expressionCommon: resCommon, originalOperator: originalOperator}, nil
		}
	}

	return nil, fmt.Errorf("ParseExpression: Invalid operator in expression %s", expr)
}

func ExpressionsAreEqual(expr1, expr2 Expression) bool {
	return expr1.GetKey() == expr2.GetKey() && expr1.GetOperator() == expr2.GetOperator() && expr1.GetValue() == expr2.GetValue()
}

// MetricDefinitionFilter takes a metric name together with its tags and returns a FilterDecision
type MetricDefinitionFilter func(id schema.MKey, name string, tags []string) FilterDecision

type FilterDecision uint8

const (
	None FilterDecision = iota // no decision has been made, because the decision might change depending on what other indexes defines
	Fail                       // it has been decided by the filter that this metric does not end up in the result set
	Pass                       // the filter has passed
)

type ExpressionOperator uint16

const (
	EQUAL       ExpressionOperator = iota // =
	NOT_EQUAL                             // !=
	MATCH                                 // =~        regular expression
	MATCH_TAG                             // __tag=~   relies on special key __tag. non-standard, required for `/metrics/tags` requests with "filter"
	NOT_MATCH                             // !=~
	PREFIX                                // ^=        exact prefix, not regex. non-standard, required for auto complete of tag values
	PREFIX_TAG                            // __tag^=   exact prefix with tag. non-standard, required for auto complete of tag keys
	HAS_TAG                               // <tag>!="" specified tag must be present
	NOT_HAS_TAG                           // <tag>="" specified tag must not be present
	MATCH_ALL                             // special case of expression that matches every metric (f.e. key=.*)
	MATCH_NONE                            // special case of expression that matches no metric (f.e. key!=.*)
)

func (o ExpressionOperator) StringIntoBuilder(builder *strings.Builder) {
	switch o {
	case EQUAL:
		builder.WriteString("=")
	case NOT_EQUAL:
		builder.WriteString("!=")
	case MATCH:
		builder.WriteString("=~")
	case MATCH_TAG:
		builder.WriteString("=~")
	case NOT_MATCH:
		builder.WriteString("!=~")
	case PREFIX:
		builder.WriteString("^=")
	case PREFIX_TAG:
		builder.WriteString("^=")
	case HAS_TAG:
		builder.WriteString("!=")
	case NOT_HAS_TAG:
		builder.WriteString("=")
	case MATCH_ALL:
		builder.WriteString("=")
	case MATCH_NONE:
		builder.WriteString("!=")
	}
}
