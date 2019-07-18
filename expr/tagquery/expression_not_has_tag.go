package tagquery

import (
	"strings"

	"github.com/raintank/schema"
)

type expressionNotHasTag struct {
	expressionCommon
}

func (e *expressionNotHasTag) GetDefaultDecision() FilterDecision {
	return Pass
}

func (e *expressionNotHasTag) GetOperator() ExpressionOperator {
	return NOT_HAS_TAG
}

func (e *expressionNotHasTag) GetCostMultiplier() uint32 {
	return 2
}

func (e *expressionNotHasTag) OperatesOnTag() bool {
	return true
}

func (e *expressionNotHasTag) RequiresNonEmptyValue() bool {
	return false
}

func (e *expressionNotHasTag) ValuePasses(value string) bool {
	return value == e.key
}

func (e *expressionNotHasTag) GetMetricDefinitionFilter(_ IdTagLookup) MetricDefinitionFilter {
	if e.key == "name" {
		return func(_ schema.MKey, _ string, _ []string) FilterDecision { return Fail }
	}

	resultIfTagIsAbsent := None
	if !metaTagSupport {
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

func (e *expressionNotHasTag) StringIntoBuilder(builder *strings.Builder) {
	builder.WriteString(e.key)
	builder.WriteString("=")
}
