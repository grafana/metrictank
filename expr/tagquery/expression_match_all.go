package tagquery

import (
	"strings"

	"github.com/raintank/schema"
)

type expressionMatchAll struct {
	// we keep key, operator, value just to be able to convert the expression back into a string
	expressionCommon
	originalOperator ExpressionOperator
}

func (e *expressionMatchAll) GetDefaultDecision() FilterDecision {
	return Pass
}

func (e *expressionMatchAll) GetKey() string {
	return e.key
}

func (e *expressionMatchAll) GetValue() string {
	return e.value
}

func (e *expressionMatchAll) GetOperator() ExpressionOperator {
	return MATCH_ALL
}

func (e *expressionMatchAll) HasRe() bool {
	return false
}

func (e *expressionMatchAll) RequiresNonEmptyValue() bool {
	return false
}

func (e *expressionMatchAll) ValuePasses(value string) bool {
	return true
}

func (e *expressionMatchAll) GetMetricDefinitionFilter(_ IdTagLookup) MetricDefinitionFilter {
	return func(id schema.MKey, name string, tags []string) FilterDecision { return Pass }
}

func (e *expressionMatchAll) StringIntoBuilder(builder *strings.Builder) {
	builder.WriteString(e.key)
	e.originalOperator.StringIntoBuilder(builder)
	builder.WriteString(e.value)
}
