package tagquery

import (
	"strings"
)

type expressionEqual struct {
	expressionCommon
}

func (e *expressionEqual) GetDefaultDecision() FilterDecision {
	return Fail
}

func (e *expressionEqual) GetOperator() ExpressionOperator {
	return EQUAL
}

func (e *expressionEqual) ValuePasses(value string) bool {
	return value == e.value
}

func (e *expressionEqual) GetMetricDefinitionFilter() MetricDefinitionFilter {
	if e.key == "name" {
		if e.value == "" {
			// every metric has a name, the value will never be empty
			return func(_ string, _ []string) FilterDecision { return Fail }
		}
		return func(name string, _ []string) FilterDecision {
			if name == e.value {
				return Pass
			}
			return Fail
		}
	}

	prefix := e.key + "="
	matchString := prefix + e.value
	return func(name string, tags []string) FilterDecision {
		for _, tag := range tags {
			if tag == matchString {
				return Pass
			}

			// the tag is set, but it has a different value,
			// no need to keep looking at other indexes
			if strings.HasPrefix(tag, prefix) {
				return Fail
			}
		}

		return None
	}
}

func (e *expressionEqual) StringIntoBuilder(builder *strings.Builder) {
	builder.WriteString(e.key)
	builder.WriteString("=")
	builder.WriteString(e.value)
}
