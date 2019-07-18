package tagquery

import (
	"strings"

	"github.com/raintank/schema"
)

type expressionPrefixTag struct {
	expressionCommon
}

func (e *expressionPrefixTag) GetDefaultDecision() FilterDecision {
	return Fail
}

func (e *expressionPrefixTag) GetOperator() ExpressionOperator {
	return PREFIX_TAG
}

func (e *expressionPrefixTag) GetCostMultiplier() uint32 {
	return 3
}

func (e *expressionPrefixTag) OperatesOnTag() bool {
	return true
}

func (e *expressionPrefixTag) RequiresNonEmptyValue() bool {
	// we know it requires an non-empty value, because the expression
	// "__tag^=" would get parsed into the type expressionMatchAll
	return true
}

func (e *expressionPrefixTag) ValuePasses(tag string) bool {
	return strings.HasPrefix(tag, e.value)
}

func (e *expressionPrefixTag) GetMetricDefinitionFilter(_ IdTagLookup) MetricDefinitionFilter {
	if strings.HasPrefix("name", e.value) {
		// every metric has a name
		return func(_ schema.MKey, _ string, _ []string) FilterDecision { return Pass }
	}

	resultIfTagIsAbsent := None
	if !metaTagSupport {
		resultIfTagIsAbsent = Fail
	}

	return func(_ schema.MKey, _ string, tags []string) FilterDecision {
		for _, tag := range tags {
			if strings.HasPrefix(tag, e.value) {
				return Pass
			}
		}
		return resultIfTagIsAbsent
	}
}

func (e *expressionPrefixTag) StringIntoBuilder(builder *strings.Builder) {
	builder.WriteString("__tag^=")
	builder.WriteString(e.value)
}
