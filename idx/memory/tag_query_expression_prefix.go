package memory

import (
	"strings"

	"github.com/grafana/metrictank/idx"
)

type expressionPrefix struct {
	expressionCommon
}

func (e *expressionPrefix) getFilter() tagFilter {
	prefix := e.key + "="
	matchString := prefix + e.value

	if e.key == "name" {
		return func(def *idx.Archive) filterDecision {
			if strings.HasPrefix(def.Name, e.value) {
				return pass
			}

			return fail
		}
	}

	return func(def *idx.Archive) filterDecision {
		for _, tag := range def.Tags {
			if strings.HasPrefix(tag, matchString) {
				return pass
			}

			if strings.HasPrefix(tag, prefix) {
				return fail
			}
		}
		return none
	}
}

func (e *expressionPrefix) hasRe() bool {
	return false
}

func (e *expressionPrefix) getMatcher() tagStringMatcher {
	return func(checkValue string) bool {
		return strings.HasPrefix(checkValue, e.value)
	}
}

func (e *expressionPrefix) matchesTag() bool {
	return false
}

func (e *expressionPrefix) getDefaultDecision() filterDecision {
	return fail
}

func (e *expressionPrefix) stringIntoBuilder(builder *strings.Builder) {
	builder.WriteString(e.key)
	builder.WriteString("^=")
	builder.WriteString(e.value)
}

func (e *expressionPrefix) getMetaRecords(mti metaTagIndex) []uint32 {
	idSet := make(map[uint32]struct{})
	match := e.getMatcher()
	if values, ok := mti[e.key]; ok {
		for value, ids := range values {
			if !match(value) {
				continue
			}
			for _, id := range ids {
				idSet[id] = struct{}{}
			}
		}
	}

	res := make([]uint32, 0, len(idSet))
	for id := range idSet {
		res = append(res, id)
	}
	return res
}
