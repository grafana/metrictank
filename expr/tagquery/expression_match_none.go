package tagquery

import (
	"strings"

	"github.com/raintank/schema"
)

type expressionMatchNone struct {
	// we keep key, operator, value just to be able to convert the expression back into a string
	expressionCommon
	originalOperator ExpressionOperator
}

func (e *expressionMatchNone) Equals(other Expression) bool {
	return e.key == other.GetKey() && e.GetOperator() == other.GetOperator() && e.value == other.GetValue()
}

func (e *expressionMatchNone) GetDefaultDecision() FilterDecision {
	return Fail
}

func (e *expressionMatchNone) GetKey() string {
	return e.key
}

func (e *expressionMatchNone) GetValue() string {
	return e.value
}

func (e *expressionMatchNone) GetOperator() ExpressionOperator {
	return MATCH_NONE
}

func (e *expressionMatchNone) GetCostMultiplier() uint32 {
	return 0
}

func (e *expressionMatchNone) RequiresNonEmptyValue() bool {
	return true
}

func (e *expressionMatchNone) Matches(value string) bool {
	return false
}

func (e *expressionMatchNone) GetMetricDefinitionFilter(_ IdTagLookup) MetricDefinitionFilter {
	return func(_ schema.MKey, _ string, _ []string) FilterDecision { return Fail }
}

func (e *expressionMatchNone) StringIntoBuilder(builder *strings.Builder) {
	builder.WriteString(e.key)
	e.originalOperator.StringIntoBuilder(builder)
	builder.WriteString(e.value)
}
