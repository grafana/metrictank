package tagQueryExpression

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

func (e *expressionMatchTag) GetMetricDefinitionFilter() MetricDefinitionFilter {
	if e.valueRe.Match([]byte("name")) {
		return func(def *idx.Archive) FilterDecision { return Pass }
	}

	var matchCache, missCache sync.Map
	var currentMatchCacheSize, currentMissCacheSize int32

	return func(def *idx.Archive) FilterDecision {
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
				return Pass
			}

			if e.valueRe.Match([]byte(value)) {
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
				continue
			}
		}

		return None
	}
}

func (e *expressionMatchTag) HasRe() bool {
	return true
}

func (e *expressionMatchTag) GetMatcher() TagStringMatcher {
	return func(tag string) bool {
		return e.valueRe.MatchString(tag)
	}
}

func (e *expressionMatchTag) MatchesTag() bool {
	return true
}

func (e *expressionMatchTag) GetDefaultDecision() FilterDecision {
	return Fail
}

func (e *expressionMatchTag) IsTagOperator() bool {
	return true
}

func (e *expressionMatchTag) StringIntoBuilder(builder *strings.Builder) {
	builder.WriteString("__tag=~")
	builder.WriteString(e.value)
}
