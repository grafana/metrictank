package tagquery

import (
	"io"
	"strings"

	"github.com/grafana/metrictank/schema"
)

type expressionHasTag struct {
	expressionCommon
}

func (e *expressionHasTag) Equals(other Expression) bool {
	return e.key == other.GetKey() && e.GetOperator() == other.GetOperator() && e.value == other.GetValue()
}

func (e *expressionHasTag) GetDefaultDecision() FilterDecision {
	return Fail
}

func (e *expressionHasTag) GetOperator() ExpressionOperator {
	return HAS_TAG
}

func (e *expressionHasTag) GetOperatorCost() uint32 {
	return 10
}

func (e *expressionHasTag) OperatesOnTag() bool {
	return true
}

func (e *expressionHasTag) Matches(value string) bool {
	return value == e.key
}

func (e *expressionHasTag) MatchesExactly() bool {
	return true
}

func (e *expressionHasTag) GetMetricDefinitionFilter(_ IdTagLookup) MetricDefinitionFilter {
	if e.key == "name" {
		return func(_ schema.MKey, _ string, _ []string) FilterDecision { return Pass }
	}

	resultIfTagIsAbsent := None
	if !MetaTagSupport {
		resultIfTagIsAbsent = Fail
	}

	matchPrefix := e.key + "="
	return func(_ schema.MKey, _ string, tags []string) FilterDecision {
		for _, tag := range tags {
			if strings.HasPrefix(tag, matchPrefix) {
				return Pass
			}
		}

		return resultIfTagIsAbsent
	}
}

func (e *expressionHasTag) StringIntoWriter(writer io.StringWriter) {
	writer.WriteString(e.key)
	writer.WriteString("!=")
}
