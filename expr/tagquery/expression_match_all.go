package tagquery

import (
	"io"

	"github.com/grafana/metrictank/schema"
)

type expressionMatchAll struct {
	// we keep key, operator, value just to be able to convert the expression back into a string
	expressionCommon
	originalOperator ExpressionOperator
}

func (e *expressionMatchAll) Equals(other Expression) bool {
	return e.key == other.GetKey() && e.GetOperator() == other.GetOperator() && e.value == other.GetValue()
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

func (e *expressionMatchAll) GetOperatorCost() uint32 {
	return 50
}

func (e *expressionMatchAll) RequiresNonEmptyValue() bool {
	return false
}

func (e *expressionMatchAll) ResultIsSmallerWhenInverted() bool {
	return true
}

func (e *expressionMatchAll) Matches(value string) bool {
	return true
}

func (e *expressionMatchAll) GetMetricDefinitionFilter(_ IdTagLookup) MetricDefinitionFilter {
	return func(_ schema.MKey, _ string, _ []string) FilterDecision { return Pass }
}

func (e *expressionMatchAll) StringIntoWriter(writer io.StringWriter) {
	writer.WriteString(e.key)
	e.originalOperator.StringIntoWriter(writer)
	writer.WriteString(e.value)
}
