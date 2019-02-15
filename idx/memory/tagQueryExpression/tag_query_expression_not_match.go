package tagQueryExpression

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

func (e *expressionNotMatch) GetMetricDefinitionFilter() MetricDefinitionFilter {
	if e.key == "name" {
		if e.value == "" {
			// every metric has a name
			return func(def *idx.Archive) FilterDecision { return Pass }
		}
		return func(def *idx.Archive) FilterDecision {
			if e.valueRe.MatchString(def.Name) {
				return Fail
			} else {
				return Pass
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

			// if value is empty, every metric which has this tag passes
			if e.value == "" {
				return Pass
			}

			value := tag[len(prefix):]

			// reduce regex matching by looking up cached non-matches
			if _, ok := missCache.Load(value); ok {
				return Pass
			}

			// reduce regex matching by looking up cached matches
			if _, ok := matchCache.Load(value); ok {
				return Fail
			}

			if e.valueRe.MatchString(value) {
				if atomic.LoadInt32(&currentMatchCacheSize) < int32(matchCacheSize) {
					matchCache.Store(value, struct{}{})
					atomic.AddInt32(&currentMatchCacheSize, 1)
				}
				return Fail
			} else {
				if atomic.LoadInt32(&currentMissCacheSize) < int32(matchCacheSize) {
					missCache.Store(value, struct{}{})
					atomic.AddInt32(&currentMissCacheSize, 1)
				}
				return Pass
			}
		}

		return None
	}
}

func (e *expressionNotMatch) HasRe() bool {
	return true
}

func (e *expressionNotMatch) GetMatcher() TagStringMatcher {
	return func(checkValue string) bool {
		return e.valueRe.MatchString(checkValue)
	}
}

func (e *expressionNotMatch) MatchesTag() bool {
	return false
}

func (e *expressionNotMatch) IsPositiveOperator() bool {
	return true
}

func (e *expressionNotMatch) GetDefaultDecision() FilterDecision {
	return Pass
}

func (e *expressionNotMatch) StringIntoBuilder(builder *strings.Builder) {
	builder.WriteString(e.key)
	builder.WriteString("!=~")
	builder.WriteString(e.value)
}
