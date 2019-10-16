package tagquery

import (
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strings"

	"github.com/grafana/metrictank/schema"
)

type InvalidExpressionError string

func (i InvalidExpressionError) Error() string {
	return fmt.Sprintf("Invalid expression: %s", string(i))
}

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
		e[i].StringIntoWriter(&builder)
		res[i] = builder.String()
		builder.Reset()
	}
	return res
}

func (e Expressions) Sort() {
	sort.Slice(e, func(i, j int) bool {
		if e[i].GetKey() == e[j].GetKey() {
			if e[i].GetOperator() == e[j].GetOperator() {
				return e[i].GetValue() < e[j].GetValue()
			}
			return e[i].GetOperator() < e[j].GetOperator()
		}
		return e[i].GetKey() < e[j].GetKey()
	})
}

// MarshalJSON satisfies the json.Marshaler interface
// it is used by the api endpoint /metaTags to list the meta tag records
func (e Expressions) MarshalJSON() ([]byte, error) {
	return json.Marshal(e.Strings())
}

func (e *Expressions) UnmarshalJSON(data []byte) error {
	var expressionStrings []string
	err := json.Unmarshal(data, &expressionStrings)
	if err != nil {
		return err
	}

	parsed, err := ParseExpressions(expressionStrings)
	if err != nil {
		return err
	}

	*e = parsed
	return nil
}

// Expression represents one expression inside a query of one or many expressions.
// It provides all the necessary methods that are required to do a tag lookup from an index keyed by
// tags & values, such as the type memory.TagIndex or the type memory.metaTagIndex.
// It is also comes with a method to generate a filter which decides whether a given MetricDefinition
// matches the requirements defined by this expression or not. This filter can be obtained from the
// method GetMetricDefinitionFilter().
type Expression interface {
	// Equals takes another expression and compares it against itself. Returns true if they are equal
	// or false otherwise
	Equals(Expression) bool

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

	// GetKey returns tag to who's values this expression get's applied to
	// example:
	// in the expression "tag1=value" GetKey() would return "tag1"
	GetKey() string

	// GetValue returns the value part of the expression
	// example:
	// in the expression "abc!=cba" this would return "cba"
	GetValue() string

	// GetOperator returns the operator of this expression
	GetOperator() ExpressionOperator

	// GetOperatorCost returns a value which should roughly reflect the cost of this operator compared
	// to other operators. F.e. = is cheaper than =~. Keep in mind that this is only a very rough
	// estimate and will never be accurate.
	GetOperatorCost() uint32

	// OperatesOnTag returns whether this expression operators on the tag key
	// (if not, it operates on the value).
	// Expressions such has expressionHasTag, expressionMatchTag, expressionPrefixTag would return true,
	// because in order to make a decision regarding whether a metric should be part of the result set
	// they need to look at a metric's tags, as opposed to looking at the values associated with some
	// specified tag.
	// If this returns true, then tags shall be passed into ValuePasses(), other values associated with
	// the tag returned by GetKey() shall be passed into ValuePasses().
	OperatesOnTag() bool

	// RequiresNonEmptyValue returns whether this expression requires a non-empty value.
	// Every valid query must have at least one expression requiring a non-empty value.
	RequiresNonEmptyValue() bool

	// ResultIsSmallerWhenInverted returns a bool indicating whether the result set after evaluating
	// this expression will likely be bigger than half of the tested index entries or smaller.
	// This is never guaranteed to actually be correct, it is only an assumption based on which we
	// can optimize performance.
	// F.e. operators = / =~ / __tag=   would return false
	//      operators != / !=~          would return true
	ResultIsSmallerWhenInverted() bool

	// Matches takes a string which should either be a tag key or value depending on the return
	// value of OperatesOnTag(), then it returns whether the given string satisfies this expression
	Matches(string) bool

	// MatchesExactly returns a bool to indicate whether the key / value of this expression (depending
	// on OperatesOnTag()) needs to be an exact match with the key / value of the metrics it evaluates
	// F.e:
	// in the case of the expression "tag1=value1" we're only looking for metrics where the value
	// associated with tag key "tag1" is exactly "value1", so a simple string comparison is sufficient.
	// in other cases like "tag1=~val.*" or "tag^=val" this isn't the case, a simple string comparison
	// is not sufficient to decide whether a metric should be part of the result set or not.
	// since simple string comparisons are cheaper than other comparison methods, whenever possible we
	// want to use string comparison.
	MatchesExactly() bool

	// GetMetricDefinitionFilter returns a MetricDefinitionFilter
	// The MetricDefinitionFilter takes a metric definition, looks at its tags and returns a decision
	// regarding this query expression applied to its tags
	GetMetricDefinitionFilter(lookup IdTagLookup) MetricDefinitionFilter

	// StringIntoWriter takes a string writer and writes a representation of this expression into it
	StringIntoWriter(writer io.StringWriter)
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
			return nil, InvalidExpressionError(expr)
		}
	}

	// key must not be empty
	if pos == 0 {
		return nil, InvalidExpressionError(expr)
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
		return nil, InvalidExpressionError(expr)
	}
	pos++

	if len(expr) > pos && expr[pos] == '~' {
		// ^=~ is not a valid operator
		if prefix {
			return nil, InvalidExpressionError(expr)
		}
		regex = true
		pos++
	}

	valuePos := pos
	for ; pos < len(expr); pos++ {
		// disallow ; in value
		if expr[pos] == ';' {
			return nil, InvalidExpressionError(expr)
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
			return nil, InvalidExpressionError(expr)
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
				return nil, InvalidExpressionError(expr)
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
				return nil, InvalidExpressionError(expr)
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

func (o ExpressionOperator) StringIntoWriter(writer io.StringWriter) {
	switch o {
	case EQUAL:
		writer.WriteString("=")
	case NOT_EQUAL:
		writer.WriteString("!=")
	case MATCH:
		writer.WriteString("=~")
	case MATCH_TAG:
		writer.WriteString("=~")
	case NOT_MATCH:
		writer.WriteString("!=~")
	case PREFIX:
		writer.WriteString("^=")
	case PREFIX_TAG:
		writer.WriteString("^=")
	case HAS_TAG:
		writer.WriteString("!=")
	case NOT_HAS_TAG:
		writer.WriteString("=")
	case MATCH_ALL:
		writer.WriteString("=")
	case MATCH_NONE:
		writer.WriteString("!=")
	}
}
