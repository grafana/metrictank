package tagQueryExpression

import (
	"strings"

	"github.com/grafana/metrictank/idx"
)

type expressionPrefixTag struct {
	expressionCommon
}

func (e *expressionPrefixTag) GetMetricDefinitionFilter() MetricDefinitionFilter {
	if strings.HasPrefix("name", e.value) {
		// every metric has a name
		return func(def *idx.Archive) FilterDecision { return Pass }
	}

	return func(def *idx.Archive) FilterDecision {
		for _, tag := range def.Tags {
			if strings.HasPrefix(tag, e.value) {
				return Pass
			}
		}
		return None
	}
}

func (e *expressionPrefixTag) HasRe() bool {
	return false
}

func (e *expressionPrefixTag) GetMatcher() TagStringMatcher {
	return func(checkTag string) bool {
		return strings.HasPrefix(checkTag, e.value)
	}
}

func (e *expressionPrefixTag) MatchesTag() bool {
	return true
}

func (e *expressionPrefixTag) GetDefaultDecision() FilterDecision {
	return Fail
}

func (e *expressionPrefixTag) IsTagOperator() bool {
	return true
}

func (e *expressionPrefixTag) StringIntoBuilder(builder *strings.Builder) {
	builder.WriteString("__tag^=")
	builder.WriteString(e.value)
}
