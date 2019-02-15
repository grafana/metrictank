package tagQueryExpression

import (
	"strings"

	"github.com/grafana/metrictank/idx"
)

type expressionNotEqual struct {
	expressionCommon
}

func (e *expressionNotEqual) GetMetricDefinitionFilter() MetricDefinitionFilter {
	if e.key == "name" {
		if e.value == "" {
			return func(def *idx.Archive) FilterDecision { return Pass }
		}
		return func(def *idx.Archive) FilterDecision {
			if def.Name == e.value {
				return Fail
			}
			return Pass
		}
	}

	prefix := e.key + "="
	matchString := prefix + e.value
	return func(def *idx.Archive) FilterDecision {
		for _, tag := range def.Tags {
			if strings.HasPrefix(tag, prefix) {
				if tag == matchString {
					return Fail
				} else {
					return Pass
				}
			}
		}
		return None
	}
}

func (e *expressionNotEqual) HasRe() bool {
	return false
}

func (e *expressionNotEqual) IsPositiveOperator() bool {
	return true
}

func (e *expressionNotEqual) GetDefaultDecision() FilterDecision {
	return Pass
}

func (e *expressionNotEqual) StringIntoBuilder(builder *strings.Builder) {
	builder.WriteString(e.key)
	builder.WriteString("!=")
	builder.WriteString(e.value)
}

func (e *expressionNotEqual) MatchesTag() bool {
	return false
}
