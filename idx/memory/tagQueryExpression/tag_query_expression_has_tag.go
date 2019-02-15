package tagQueryExpression

import (
	"strings"

	"github.com/grafana/metrictank/idx"
)

type expressionHasTag struct {
	expressionCommon
}

func (e *expressionHasTag) GetMetricDefinitionFilter() MetricDefinitionFilter {
	if e.key == "name" {
		return func(def *idx.Archive) FilterDecision { return Pass }
	}
	return func(def *idx.Archive) FilterDecision {
		matchPrefix := e.GetKey() + "="
		for _, tag := range def.Tags {
			if strings.HasPrefix(tag, matchPrefix) {
				return Pass
			}
		}
		return None
	}
}

func (e *expressionHasTag) HasRe() bool {
	return false
}

func (e *expressionHasTag) GetMatcher() TagStringMatcher {
	// in the case of the opHasTag operator the value just doesn't matter at all
	return func(checkValue string) bool {
		return true
	}
}

func (e *expressionHasTag) MatchesTag() bool {
	return false
}

func (e *expressionHasTag) GetDefaultDecision() FilterDecision {
	return Fail
}

func (e *expressionHasTag) IsTagOperator() bool {
	return true
}

func (e *expressionHasTag) StringIntoBuilder(builder *strings.Builder) {
	builder.WriteString(e.key)
	builder.WriteString("!=")
}
