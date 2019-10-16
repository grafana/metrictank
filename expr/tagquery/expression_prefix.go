package tagquery

import (
	"io"
	"strings"

	"github.com/grafana/metrictank/schema"
)

type expressionPrefix struct {
	expressionCommon
}

func (e *expressionPrefix) Equals(other Expression) bool {
	return e.key == other.GetKey() && e.GetOperator() == other.GetOperator() && e.value == other.GetValue()
}

func (e *expressionPrefix) GetDefaultDecision() FilterDecision {
	return Fail
}

func (e *expressionPrefix) GetOperator() ExpressionOperator {
	return PREFIX
}

func (e *expressionPrefix) GetOperatorCost() uint32 {
	return 2
}

func (e *expressionPrefix) RequiresNonEmptyValue() bool {
	// we know it requires an non-empty value, because the expression
	// "__tag^=" would get parsed into the type expressionMatchAll
	return true
}

func (e *expressionPrefix) Matches(value string) bool {
	return strings.HasPrefix(value, e.value)
}

func (e *expressionPrefix) GetMetricDefinitionFilter(_ IdTagLookup) MetricDefinitionFilter {
	prefix := e.key + "="
	matchString := prefix + e.value

	if e.key == "name" {
		return func(_ schema.MKey, name string, _ []string) FilterDecision {
			if strings.HasPrefix(schema.SanitizeNameAsTagValue(name), e.value) {
				return Pass
			}

			return Fail
		}
	}

	resultIfTagIsAbsent := None
	if !MetaTagSupport {
		resultIfTagIsAbsent = Fail
	}

	return func(_ schema.MKey, _ string, tags []string) FilterDecision {
		for _, tag := range tags {
			if strings.HasPrefix(tag, matchString) {
				return Pass
			}

			if strings.HasPrefix(tag, prefix) {
				return Fail
			}
		}

		return resultIfTagIsAbsent
	}
}

func (e *expressionPrefix) StringIntoWriter(writer io.StringWriter) {
	writer.WriteString(e.key)
	writer.WriteString("^=")
	writer.WriteString(e.value)
}
