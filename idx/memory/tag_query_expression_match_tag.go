package memory

import (
	"regexp"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/grafana/metrictank/idx"
)

type expressionMatchTag struct {
	expressionCommon
	valueRe *regexp.Regexp
}

func (e *expressionMatchTag) getFilter() tagFilter {
	if e.valueRe.Match([]byte("name")) {
		return func(def *idx.Archive) filterDecision { return pass }
	}

	var matchCache, missCache sync.Map
	var currentMatchCacheSize, currentMissCacheSize int32

	return func(def *idx.Archive) filterDecision {
		for _, tag := range def.Tags {
			values := strings.SplitN(tag, "=", 2)
			if len(values) < 2 {
				continue
			}
			value := values[0]

			if _, ok := missCache.Load(value); ok {
				continue
			}

			if _, ok := matchCache.Load(value); ok {
				return pass
			}

			if e.valueRe.Match([]byte(value)) {
				if atomic.LoadInt32(&currentMatchCacheSize) < int32(matchCacheSize) {
					matchCache.Store(value, struct{}{})
					atomic.AddInt32(&currentMatchCacheSize, 1)
				}
				return pass
			} else {
				if atomic.LoadInt32(&currentMissCacheSize) < int32(matchCacheSize) {
					missCache.Store(value, struct{}{})
					atomic.AddInt32(&currentMissCacheSize, 1)
				}
				continue
			}
		}

		return none
	}
}

func (e *expressionMatchTag) hasRe() bool {
	return true
}

func (e *expressionMatchTag) getMatcher() tagStringMatcher {
	return func(tag string) bool {
		return e.valueRe.MatchString(tag)
	}
}

func (e *expressionMatchTag) matchesTag() bool {
	return true
}

func (e *expressionMatchTag) getDefaultDecision() filterDecision {
	return fail
}

func (e *expressionMatchTag) stringIntoBuilder(builder *strings.Builder) {
	builder.WriteString("__tag=~")
	builder.WriteString(e.value)
}

func (e *expressionMatchTag) getMetaRecords(mti metaTagIndex) []uint32 {
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

func (e *expressionMatchTag) getMetaRecordFilter(evaluators []metaRecordEvaluator) tagFilter {
	return e.expressionCommon.getMetaRecordFilterWithDecision(evaluators, pass)
}
