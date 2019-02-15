package memory

import (
	"strings"

	"github.com/grafana/metrictank/idx"
)

type expressionNotEqual struct {
	expressionCommon
}

func (e *expressionNotEqual) getFilter() tagFilter {
	if e.key == "name" {
		if e.value == "" {
			return func(def *idx.Archive) filterDecision { return pass }
		}
		return func(def *idx.Archive) filterDecision {
			if def.Name == e.value {
				return fail
			}
			return pass
		}
	}

	prefix := e.key + "="
	matchString := prefix + e.value
	return func(def *idx.Archive) filterDecision {
		for _, tag := range def.Tags {
			if strings.HasPrefix(tag, prefix) {
				if tag == matchString {
					return fail
				} else {
					return pass
				}
			}
		}
		return none
	}
}

func (e *expressionNotEqual) hasRe() bool {
	return false
}

func (e *expressionNotEqual) getDefaultDecision() filterDecision {
	return pass
}

func (e *expressionNotEqual) stringIntoBuilder(builder *strings.Builder) {
	builder.WriteString(e.key)
	builder.WriteString("!=")
	builder.WriteString(e.value)
}

func (e *expressionNotEqual) matchesTag() bool {
	return false
}
