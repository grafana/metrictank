package tagquery

import (
	"strings"

	"github.com/raintank/schema"
)

type expressionNotEqual struct {
	expressionCommon
}

func (e *expressionNotEqual) GetDefaultDecision() FilterDecision {
	return Pass
}

func (e *expressionNotEqual) GetOperator() ExpressionOperator {
	return NOT_EQUAL
}

func (e *expressionNotEqual) RequiresNonEmptyValue() bool {
	return false
}

func (e *expressionNotEqual) ValuePasses(value string) bool {
	return value != e.value
}

func (e *expressionNotEqual) GetMetricDefinitionFilter() MetricDefinitionFilter {
	if e.key == "name" {
		if e.value == "" {
			return func(_ string, _ []string) FilterDecision { return Pass }
		}
		return func(name string, _ []string) FilterDecision {
			if schema.SanitizeNameAsTagValue(name) == e.value {
				return Fail
			}
			return Pass
		}
	}

	prefix := e.key + "="
	matchString := prefix + e.value
	return func(_ string, tags []string) FilterDecision {
		for _, tag := range tags {
			if strings.HasPrefix(tag, prefix) {
				if tag == matchString {
					return Fail
				} else {
					return Pass
				}
			}
		}
		return None
	}
}

func (e *expressionNotEqual) StringIntoBuilder(builder *strings.Builder) {
	builder.WriteString(e.key)
	builder.WriteString("!=")
	builder.WriteString(e.value)
}
