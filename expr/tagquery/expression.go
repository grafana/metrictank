package tagquery

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
)

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

// Sort sorts all the expressions first by key, then by value, then by operator
func (e Expressions) Sort() {
	sort.Slice(e, func(i, j int) bool {
		if e[i].Key == e[j].Key {
			if e[i].Value == e[j].Value {
				return e[i].Operator < e[j].Operator
			}
			return e[i].Value < e[j].Value
		}
		return e[i].Key < e[j].Key
	})
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

type Expression struct {
	Tag
	Operator              ExpressionOperator
	RequiresNonEmptyValue bool
	UsesRegex             bool
	Regex                 *regexp.Regexp
}

// ParseQueryExpression takes a tag query expression as a string and validates it
func ParseExpression(expression string) (Expression, error) {
	var operatorStartPos, operatorEndPos, equalPos int
	var res Expression

	equalPos = strings.Index(expression, "=")
	if equalPos < 0 {
		return res, fmt.Errorf("Missing equal sign: %s", expression)
	}

	if equalPos == 0 {
		return res, fmt.Errorf("Empty tag key: %s", expression)
	}

	res.RequiresNonEmptyValue = true
	if expression[equalPos-1] == '!' {
		operatorStartPos = equalPos - 1
		res.RequiresNonEmptyValue = false
		res.Operator = NOT_EQUAL
	} else if expression[equalPos-1] == '^' {
		operatorStartPos = equalPos - 1
		res.Operator = PREFIX
	} else {
		operatorStartPos = equalPos
		res.Operator = EQUAL
	}

	res.Key = expression[:operatorStartPos]
	err := validateQueryExpressionTagKey(res.Key)
	if err != nil {
		return res, fmt.Errorf("Error when validating key \"%s\" of expression \"%s\": %s", res.Key, expression, err)
	}

	res.UsesRegex = false
	if len(expression)-1 == equalPos {
		operatorEndPos = equalPos
	} else if expression[equalPos+1] == '~' {
		operatorEndPos = equalPos + 1
		res.UsesRegex = true

		switch res.Operator {
		case EQUAL:
			res.Operator = MATCH
		case NOT_EQUAL:
			res.Operator = NOT_MATCH
		case PREFIX:
			return res, fmt.Errorf("The string \"^=~\" is not a valid operator in expression %s", expression)
		}
	} else {
		operatorEndPos = equalPos
	}

	res.Value = expression[operatorEndPos+1:]

	if res.UsesRegex {
		if len(res.Value) > 0 && res.Value[0] != '^' {
			// always anchor all regular expressions at the beginning if they do not start with ^
			res.Value = "^(?:" + res.Value + ")"
		}

		res.Regex, err = regexp.Compile(res.Value)
		if err != nil {
			return res, fmt.Errorf("Invalid regular expression given as value %s in expression %s: %s", res.Value, expression, err)
		}

		if res.Regex.Match(nil) {
			// if value matches empty string, then requiresNonEmptyValue gets negated
			res.RequiresNonEmptyValue = !res.RequiresNonEmptyValue
		}
	} else {
		if len(res.Value) == 0 {
			// if value is empty, then requiresNonEmptyValue gets negated
			// f.e.
			// tag1!= means there must be a tag "tag1", instead of there must not be
			// tag1= means there must not be a "tag1", instead of there must be
			res.RequiresNonEmptyValue = !res.RequiresNonEmptyValue
		}
	}

	if res.Key == "__tag" {
		if len(res.Value) == 0 {
			return res, errInvalidQuery
		}

		if res.Operator == PREFIX {
			res.Operator = PREFIX_TAG
		} else if res.Operator == MATCH {
			res.Operator = MATCH_TAG
		} else {
			return res, errInvalidQuery
		}
	}

	return res, nil
}

func (e *Expression) IsEqualTo(other Expression) bool {
	return e.Key == other.Key && e.Operator == other.Operator && e.Value == other.Value
}

func (e *Expression) StringIntoBuilder(builder *strings.Builder) {
	if e.Operator == MATCH_TAG || e.Operator == PREFIX_TAG {
		builder.WriteString("__tag")
	} else {
		builder.WriteString(e.Key)
	}
	e.Operator.StringIntoBuilder(builder)
	builder.WriteString(e.Value)
}

type ExpressionOperator uint16

const (
	EQUAL      ExpressionOperator = iota // =
	NOT_EQUAL                            // !=
	MATCH                                // =~        regular expression
	MATCH_TAG                            // __tag=~   relies on special key __tag. non-standard, required for `/metrics/tags` requests with "filter"
	NOT_MATCH                            // !=~
	PREFIX                               // ^=        exact prefix, not regex. non-standard, required for auto complete of tag values
	PREFIX_TAG                           // __tag^=   exact prefix with tag. non-standard, required for auto complete of tag keys
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
	}
}
