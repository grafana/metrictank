package tagQueryExpression

import (
	"strings"

	"github.com/grafana/metrictank/idx"
)

type expressionPrefix struct {
	expressionCommon
}

func (e *expressionPrefix) GetMetricDefinitionFilter() MetricDefinitionFilter {
	prefix := e.key + "="
	matchString := prefix + e.value

	if e.key == "name" {
		return func(def *idx.Archive) FilterDecision {
			if strings.HasPrefix(def.Name, e.value) {
				return Pass
			}

			return Fail
		}
	}

	return func(def *idx.Archive) FilterDecision {
		for _, tag := range def.Tags {
			if strings.HasPrefix(tag, matchString) {
				return Pass
			}

			if strings.HasPrefix(tag, prefix) {
				return Fail
			}
		}
		return None
	}
}

func (e *expressionPrefix) HasRe() bool {
	return false
}

func (e *expressionPrefix) GetMatcher() TagStringMatcher {
	return func(checkValue string) bool {
		return strings.HasPrefix(checkValue, e.value)
	}
}

func (e *expressionPrefix) MatchesTag() bool {
	return false
}

func (e *expressionPrefix) GetDefaultDecision() FilterDecision {
	return Fail
}

func (e *expressionPrefix) StringIntoBuilder(builder *strings.Builder) {
	builder.WriteString(e.key)
	builder.WriteString("^=")
	builder.WriteString(e.value)
}
