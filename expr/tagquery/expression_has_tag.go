package tagquery

import (
	"strings"
)

type expressionHasTag struct {
	expressionCommon
}

func (e *expressionHasTag) GetOperator() ExpressionOperator {
	return HAS_TAG
}

func (e *expressionHasTag) ValuePasses(value string) bool {
	return value == e.key
}

func (e *expressionHasTag) GetDefaultDecision() FilterDecision {
	return Fail
}

func (e *expressionHasTag) OperatesOnTag() bool {
	return true
}

func (e *expressionHasTag) StringIntoBuilder(builder *strings.Builder) {
	builder.WriteString(e.key)
	builder.WriteString("!=")
}

func (e *expressionHasTag) GetMetricDefinitionFilter() MetricDefinitionFilter {
	if e.key == "name" {
		return func(_ string, _ []string) FilterDecision { return Pass }
	}

	matchPrefix := e.key + "="
	return func(_ string, tags []string) FilterDecision {
		for _, tag := range tags {
			if strings.HasPrefix(tag, matchPrefix) {
				return Pass
			}
		}

		return None
	}
}
