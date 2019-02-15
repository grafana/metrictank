package tagQueryExpression

import (
	"strings"

	"github.com/grafana/metrictank/idx"
)

type expressionEqual struct {
	expressionCommon
}

func (e *expressionEqual) GetMetricDefinitionFilter() MetricDefinitionFilter {
	if e.key == "name" {
		if e.value == "" {
			// every metric has a name, the value will never be empty
			return func(*idx.Archive) FilterDecision { return Fail }
		}
		return func(def *idx.Archive) FilterDecision {
			if def.Name == e.value {
				return Pass
			}
			return Fail
		}
	}

	prefix := e.key + "="
	matchString := prefix + e.value
	return func(def *idx.Archive) FilterDecision {
		for _, tag := range def.Tags {
			if tag == matchString {
				return Pass
			}

			// the tag is set, but it has a different value, no need to keep looking
			if strings.HasPrefix(tag, prefix) {
				return Fail
			}
		}

		return None
	}
}

func (e *expressionEqual) HasRe() bool {
	return false
}

func (e *expressionEqual) GetMatcher() TagStringMatcher {
	return func(checkValue string) bool {
		return checkValue == e.value
	}
}

func (e *expressionEqual) MatchesTag() bool {
	return false
}

func (e *expressionEqual) GetDefaultDecision() FilterDecision {
	return Fail
}

func (e *expressionEqual) StringIntoBuilder(builder *strings.Builder) {
	builder.WriteString(e.key)
	builder.WriteString("=")
	builder.WriteString(e.value)
}
