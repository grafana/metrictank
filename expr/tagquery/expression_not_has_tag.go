package tagquery

import (
	"strings"
)

type expressionNotHasTag struct {
	expressionCommon
}

func (e *expressionNotHasTag) GetOperator() ExpressionOperator {
	return NOT_HAS_TAG
}

func (e *expressionNotHasTag) RequiresNonEmptyValue() bool {
	return false
}

func (e *expressionNotHasTag) ValuePasses(value string) bool {
	return value == e.key
}

func (e *expressionNotHasTag) GetDefaultDecision() FilterDecision {
	return Pass
}

func (e *expressionNotHasTag) OperatesOnTag() bool {
	return true
}

func (e *expressionNotHasTag) StringIntoBuilder(builder *strings.Builder) {
	builder.WriteString(e.key)
	builder.WriteString("=")
}

func (e *expressionNotHasTag) GetMetricDefinitionFilter() MetricDefinitionFilter {
	if e.key == "name" {
		return func(_ string, _ []string) FilterDecision { return Fail }
	}

	matchPrefix := e.key + "="
	return func(_ string, tags []string) FilterDecision {
		for _, tag := range tags {
			if strings.HasPrefix(tag, matchPrefix) {
				return Fail
			}
		}
		return None
	}
}
