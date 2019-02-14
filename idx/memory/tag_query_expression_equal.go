package memory

import (
	"strings"

	"github.com/grafana/metrictank/idx"
)

type expressionEqual struct {
	expressionCommon
}

func (e *expressionEqual) getFilter() tagFilter {
	if e.key == "name" {
		if e.value == "" {
			// every metric has a name, the value will never be empty
			return func(def *idx.Archive) filterDecision { return fail }
		}
		return func(def *idx.Archive) filterDecision {
			if def.Name == e.value {
				return pass
			}
			return fail
		}
	}

	prefix := e.key + "="
	matchString := prefix + e.value
	return func(def *idx.Archive) filterDecision {
		for _, tag := range def.Tags {
			if tag == matchString {
				return pass
			}

			// the tag is set, but it has a different value, no need to keep looking
			if strings.HasPrefix(tag, prefix) {
				return fail
			}
		}

		return none
	}
}

func (e *expressionEqual) hasRe() bool {
	return false
}

func (e *expressionEqual) getMatcher() tagStringMatcher {
	return func(checkValue string) bool {
		return checkValue == e.value
	}

}

func (e *expressionEqual) matchesTag() bool {
	return false
}

func (e *expressionEqual) getDefaultDecision() filterDecision {
	return fail
}

func (e *expressionEqual) stringIntoBuilder(builder *strings.Builder) {
	builder.WriteString(e.key)
	builder.WriteString("=")
	builder.WriteString(e.value)
}

func (e *expressionEqual) getMetaRecords(mti metaTagIndex) []uint32 {
	if values, ok := mti[e.key]; ok {
		if ids, ok := values[e.value]; ok {
			return ids
		}
	}

	return nil
}

func (e *expressionEqual) getMetaRecordFilter(evaluators []metaRecordEvaluator) tagFilter {
	return e.expressionCommon.getMetaRecordFilterWithDecision(evaluators, pass)
}
