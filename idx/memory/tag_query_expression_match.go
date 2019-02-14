package memory

import (
	"regexp"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/grafana/metrictank/idx"
)

type expressionMatch struct {
	expressionCommon
	valueRe *regexp.Regexp
}

func (e *expressionMatch) getFilter() tagFilter {
	if e.key == "name" {
		if e.value == "" {
			// silly query, always fails
			return func(def *idx.Archive) filterDecision { return fail }
		}
		return func(def *idx.Archive) filterDecision {
			if e.valueRe.MatchString(def.Name) {
				return pass
			} else {
				return fail
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

			// if value is empty, every metric which has this tag fails
			if e.value == "" {
				return fail
			}

			value := tag[len(prefix):]

			// reduce regex matching by looking up cached non-matches
			if _, ok := missCache.Load(value); ok {
				return fail
			}

			// reduce regex matching by looking up cached matches
			if _, ok := matchCache.Load(value); ok {
				return pass
			}

			if e.valueRe.MatchString(value) {
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
				return fail
			}
		}

		return none
	}
}

func (e *expressionMatch) hasRe() bool {
	return true
}

func (e *expressionMatch) getMatcher() tagStringMatcher {
	return func(checkValue string) bool {
		return e.valueRe.MatchString(checkValue)
	}
}

func (e *expressionMatch) matchesTag() bool {
	return false
}

func (e *expressionMatch) getDefaultDecision() filterDecision {
	return fail
}

func (e *expressionMatch) stringIntoBuilder(builder *strings.Builder) {
	builder.WriteString(e.key)
	builder.WriteString("=~")
	builder.WriteString(e.value)
}

func (e *expressionMatch) getMetaRecords(mti metaTagIndex) []uint32 {
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

func (e *expressionMatch) getMetaRecordFilter(evaluators []metaRecordEvaluator) tagFilter {
	return e.expressionCommon.getMetaRecordFilterWithDecision(evaluators, pass)
}
