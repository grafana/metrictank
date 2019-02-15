package tagQueryExpression

import (
	"strings"

	"github.com/grafana/metrictank/idx"
)

type expressionNotHasTag struct {
	expressionCommon
}

func (e *expressionNotHasTag) GetMetricDefinitionFilter() MetricDefinitionFilter {
	if e.key == "name" {
		return func(def *idx.Archive) FilterDecision { return Fail }
	}
	return func(def *idx.Archive) FilterDecision {
		matchPrefix := e.GetKey() + "="
		for _, tag := range def.Tags {
			if strings.HasPrefix(tag, matchPrefix) {
				return Fail
			}
		}
		return None
	}
}

func (e *expressionNotHasTag) HasRe() bool {
	return false
}

func (e *expressionNotHasTag) GetMatcher() TagStringMatcher {
	// in the case of the opNotHasTag operator the value just doesn't matter at all
	return func(checkValue string) bool {
		return false
	}
}

func (e *expressionNotHasTag) MatchesTag() bool {
	return true
}

func (e *expressionNotHasTag) IsPositiveOperator() bool {
	return true
}

func (e *expressionNotHasTag) GetDefaultDecision() FilterDecision {
	return Pass
}

func (e *expressionNotHasTag) StringIntoBuilder(builder *strings.Builder) {
	builder.WriteString(e.key)
	builder.WriteString("=")
}
