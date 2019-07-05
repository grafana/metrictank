package tagquery

import (
	"strings"
)

type expressionPrefix struct {
	expressionCommon
}

func (e *expressionPrefix) GetOperator() ExpressionOperator {
	return PREFIX
}

func (e *expressionPrefix) ValuePasses(value string) bool {
	return strings.HasPrefix(value, e.value)
}

func (e *expressionPrefix) RequiresNonEmptyValue() bool {
	// we know it requires an non-empty value, because the expression
	// "__tag^=" would get parsed into the type expressionMatchAll
	return true
}

func (e *expressionPrefix) GetDefaultDecision() FilterDecision {
	return Fail
}

func (e *expressionPrefix) StringIntoBuilder(builder *strings.Builder) {
	builder.WriteString(e.key)
	builder.WriteString("^=")
	builder.WriteString(e.value)
}

func (e *expressionPrefix) GetMetricDefinitionFilter() MetricDefinitionFilter {
	prefix := e.key + "="
	matchString := prefix + e.value

	if e.key == "name" {
		return func(name string, _ []string) FilterDecision {
			if strings.HasPrefix(name, e.value) {
				return Pass
			}

			return Fail
		}
	}

	return func(_ string, tags []string) FilterDecision {
		for _, tag := range tags {
			if strings.HasPrefix(tag, matchString) {
				return Pass
			}

			if strings.HasPrefix(tag, prefix) {
				return Fail
			}
		}

		return None
	}
}
