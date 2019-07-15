package tagquery

import (
	"strings"
)

type expressionPrefixTag struct {
	expressionCommon
}

func (e *expressionPrefixTag) GetDefaultDecision() FilterDecision {
	return Fail
}

func (e *expressionPrefixTag) GetOperator() ExpressionOperator {
	return PREFIX_TAG
}

func (e *expressionPrefixTag) OperatesOnTag() bool {
	return true
}

func (e *expressionPrefixTag) RequiresNonEmptyValue() bool {
	// we know it requires an non-empty value, because the expression
	// "__tag^=" would get parsed into the type expressionMatchAll
	return true
}

func (e *expressionPrefixTag) ValuePasses(tag string) bool {
	return strings.HasPrefix(tag, e.value)
}

func (e *expressionPrefixTag) GetMetricDefinitionFilter() MetricDefinitionFilter {
	if strings.HasPrefix("name", e.value) {
		// every metric has a name
		return func(_ string, _ []string) FilterDecision { return Pass }
	}

	return func(_ string, tags []string) FilterDecision {
		for _, tag := range tags {
			if strings.HasPrefix(tag, e.value) {
				return Pass
			}
		}
		return None
	}
}

func (e *expressionPrefixTag) StringIntoBuilder(builder *strings.Builder) {
	builder.WriteString("__tag^=")
	builder.WriteString(e.value)
}
