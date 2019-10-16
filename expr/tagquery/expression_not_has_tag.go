package tagquery

import (
	"io"
	"strings"

	"github.com/grafana/metrictank/schema"
)

type expressionNotHasTag struct {
	expressionCommon
}

func (e *expressionNotHasTag) Equals(other Expression) bool {
	return e.key == other.GetKey() && e.GetOperator() == other.GetOperator() && e.value == other.GetValue()
}

func (e *expressionNotHasTag) GetDefaultDecision() FilterDecision {
	return Pass
}

func (e *expressionNotHasTag) GetOperator() ExpressionOperator {
	return NOT_HAS_TAG
}

func (e *expressionNotHasTag) GetOperatorCost() uint32 {
	return 10
}

func (e *expressionNotHasTag) OperatesOnTag() bool {
	return true
}

func (e *expressionNotHasTag) RequiresNonEmptyValue() bool {
	return false
}

func (e *expressionNotHasTag) ResultIsSmallerWhenInverted() bool {
	return true
}

func (e *expressionNotHasTag) Matches(value string) bool {
	return value != e.key
}

func (e *expressionNotHasTag) GetMetricDefinitionFilter(_ IdTagLookup) MetricDefinitionFilter {
	if e.key == "name" {
		return func(_ schema.MKey, _ string, _ []string) FilterDecision { return Fail }
	}

	resultIfTagIsAbsent := None
	if !MetaTagSupport {
		resultIfTagIsAbsent = Pass
	}

	matchPrefix := e.key + "="
	return func(_ schema.MKey, _ string, tags []string) FilterDecision {
		for _, tag := range tags {
			if strings.HasPrefix(tag, matchPrefix) {
				return Fail
			}
		}

		return resultIfTagIsAbsent
	}
}

func (e *expressionNotHasTag) StringIntoWriter(writer io.StringWriter) {
	writer.WriteString(e.key)
	writer.WriteString("=")
}
