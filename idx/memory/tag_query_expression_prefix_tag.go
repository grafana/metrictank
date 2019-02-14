package memory

import (
	"strings"

	"github.com/grafana/metrictank/idx"
)

type expressionPrefixTag struct {
	expressionCommon
}

func (e *expressionPrefixTag) getFilter() tagFilter {
	if strings.HasPrefix("name", e.value) {
		// every metric has a name
		return func(def *idx.Archive) filterDecision { return pass }
	}

	return func(def *idx.Archive) filterDecision {
		for _, tag := range def.Tags {
			if strings.HasPrefix(tag, e.value) {
				return pass
			}
		}
		return none
	}
}

func (e *expressionPrefixTag) hasRe() bool {
	return false
}

func (e *expressionPrefixTag) getMatcher() tagStringMatcher {
	return func(checkTag string) bool {
		return strings.HasPrefix(checkTag, e.value)
	}
}

func (e *expressionPrefixTag) matchesTag() bool {
	return true
}

func (e *expressionPrefixTag) getDefaultDecision() filterDecision {
	return fail
}

func (e *expressionPrefixTag) stringIntoBuilder(builder *strings.Builder) {
	builder.WriteString("__tag^=")
	builder.WriteString(e.value)
}

func (e *expressionPrefixTag) getMetaRecords(mti metaTagIndex) []uint32 {
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

func (e *expressionPrefixTag) getMetaRecordFilter(evaluators []metaRecordEvaluator) tagFilter {
	return e.expressionCommon.getMetaRecordFilterWithDecision(evaluators, pass)
}
