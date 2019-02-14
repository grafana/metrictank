package memory

import (
	"regexp"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/grafana/metrictank/idx"
)

type expressionNotMatch struct {
	expressionCommon
	valueRe *regexp.Regexp
}

func (e *expressionNotMatch) getFilter() tagFilter {
	if e.key == "name" {
		if e.value == "" {
			// every metric has a name
			return func(def *idx.Archive) filterDecision { return pass }
		}
		return func(def *idx.Archive) filterDecision {
			if e.valueRe.MatchString(def.Name) {
				return fail
			} else {
				return pass
			}
		}
	}

	var matchCache, missCache sync.Map
	var currentMatchCacheSize, currentMissCacheSize int32
	prefix := e.key + "="

	return func(def *idx.Archive) filterDecision {
		for _, tag := range def.Tags {
			if !strings.HasPrefix(tag, prefix) {
				continue
			}

			// if value is empty, every metric which has this tag passes
			if e.value == "" {
				return pass
			}

			value := tag[len(prefix):]

			// reduce regex matching by looking up cached non-matches
			if _, ok := missCache.Load(value); ok {
				return pass
			}

			// reduce regex matching by looking up cached matches
			if _, ok := matchCache.Load(value); ok {
				return fail
			}

			if e.valueRe.MatchString(value) {
				if atomic.LoadInt32(&currentMatchCacheSize) < int32(matchCacheSize) {
					matchCache.Store(value, struct{}{})
					atomic.AddInt32(&currentMatchCacheSize, 1)
				}
				return fail
			} else {
				if atomic.LoadInt32(&currentMissCacheSize) < int32(matchCacheSize) {
					missCache.Store(value, struct{}{})
					atomic.AddInt32(&currentMissCacheSize, 1)
				}
				return pass
			}
		}

		return none
	}
}

func (e *expressionNotMatch) hasRe() bool {
	return true
}

func (e *expressionNotMatch) getMatcher() tagStringMatcher {
	return func(checkValue string) bool {
		return e.valueRe.MatchString(checkValue)
	}
}

func (e *expressionNotMatch) matchesTag() bool {
	return false
}

func (e *expressionNotMatch) getDefaultDecision() filterDecision {
	return pass
}

func (e *expressionNotMatch) stringIntoBuilder(builder *strings.Builder) {
	builder.WriteString(e.key)
	builder.WriteString("!=~")
	builder.WriteString(e.value)
}

func (e *expressionNotMatch) getMetaRecords(mti metaTagIndex) []uint32 {
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

func (e *expressionNotMatch) getMetaRecordFilter(evaluators []metaRecordEvaluator) tagFilter {
	return e.expressionCommon.getMetaRecordFilterWithDecision(evaluators, none)
}
