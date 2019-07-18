package tagquery

import (
	"strings"

	"github.com/raintank/schema"
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

func (e *expressionEqual) GetCostMultiplier() uint32 {
	return 1
}

func (e *expressionEqual) ValuePasses(value string) bool {
	return value == e.value
}

func (e *expressionEqual) ValueMatchesExactly() bool {
	return true
}

func (e *expressionEqual) GetMetricDefinitionFilter(lookup IdTagLookup) MetricDefinitionFilter {
	if e.key == "name" {
		if e.value == "" {
			// every metric has a name, the value will never be empty
			return func(_ schema.MKey, _ string, _ []string) FilterDecision { return Fail }
		}

		return func(_ schema.MKey, name string, _ []string) FilterDecision {
			if name == e.value {
				return Pass
			}
			return Fail
		}
	}

	if !metaTagSupport {
		return func(id schema.MKey, _ string, _ []string) FilterDecision {
			if lookup(id, e.key, e.value) {
				return Pass
			}
			return Fail
		}
	}

	prefix := e.key + "="
	return func(id schema.MKey, _ string, tags []string) FilterDecision {
		if lookup(id, e.key, e.value) {
			return Pass
		}

		for _, tag := range tags {
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
