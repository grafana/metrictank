package tagQueryExpression

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

func (e *expressionMatch) GetMetricDefinitionFilter() MetricDefinitionFilter {
	if e.key == "name" {
		if e.value == "" {
			// silly query, always fails
			return func(def *idx.Archive) FilterDecision { return Fail }
		}
		return func(def *idx.Archive) FilterDecision {
			if e.valueRe.MatchString(def.Name) {
				return Pass
			} else {
				return Fail
			}
		}
	}

	var matchCache, missCache sync.Map
	var currentMatchCacheSize, currentMissCacheSize int32
	prefix := e.key + "="

	return func(def *idx.Archive) FilterDecision {
		for _, tag := range def.Tags {
			if !strings.HasPrefix(tag, prefix) {
				continue
			}

			// if value is empty, every metric which has this tag fails
			if e.value == "" {
				return Fail
			}

			value := tag[len(prefix):]

			// reduce regex matching by looking up cached non-matches
			if _, ok := missCache.Load(value); ok {
				return Fail
			}

			// reduce regex matching by looking up cached matches
			if _, ok := matchCache.Load(value); ok {
				return Pass
			}

			if e.valueRe.MatchString(value) {
				if atomic.LoadInt32(&currentMatchCacheSize) < int32(matchCacheSize) {
					matchCache.Store(value, struct{}{})
					atomic.AddInt32(&currentMatchCacheSize, 1)
				}
				return Pass
			} else {
				if atomic.LoadInt32(&currentMissCacheSize) < int32(matchCacheSize) {
					missCache.Store(value, struct{}{})
					atomic.AddInt32(&currentMissCacheSize, 1)
				}
				return Fail
			}
		}

		return None
	}
}

func (e *expressionMatch) HasRe() bool {
	return true
}

func (e *expressionMatch) GetMatcher() TagStringMatcher {
	return func(checkValue string) bool {
		return e.valueRe.MatchString(checkValue)
	}
}

func (e *expressionMatch) MatchesTag() bool {
	return false
}

func (e *expressionMatch) GetDefaultDecision() FilterDecision {
	return Fail
}

func (e *expressionMatch) StringIntoBuilder(builder *strings.Builder) {
	builder.WriteString(e.key)
	builder.WriteString("=~")
	builder.WriteString(e.value)
}
