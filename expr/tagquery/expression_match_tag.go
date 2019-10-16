package tagquery

import (
	"io"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/grafana/metrictank/schema"
)

type expressionMatchTag struct {
	expressionCommonRe
}

func (e *expressionMatchTag) Equals(other Expression) bool {
	return e.key == other.GetKey() && e.GetOperator() == other.GetOperator() && e.value == other.GetValue()
}

func (e *expressionMatchTag) GetDefaultDecision() FilterDecision {
	return Fail
}

func (e *expressionMatchTag) GetOperator() ExpressionOperator {
	return MATCH_TAG
}

func (e *expressionMatchTag) GetOperatorCost() uint32 {
	return 20
}

func (e *expressionMatchTag) OperatesOnTag() bool {
	return true
}

func (e *expressionMatchTag) RequiresNonEmptyValue() bool {
	return !e.matchesEmpty
}

func (e *expressionMatchTag) Matches(tag string) bool {
	return e.valueRe.MatchString(tag)
}

func (e *expressionMatchTag) GetMetricDefinitionFilter(_ IdTagLookup) MetricDefinitionFilter {
	if e.valueRe.Match([]byte("name")) {
		// every metric has a tag name, so we can always return Pass
		return func(_ schema.MKey, _ string, _ []string) FilterDecision { return Pass }
	}

	resultIfTagIsAbsent := None
	if !MetaTagSupport {
		resultIfTagIsAbsent = Fail
	}

	var matchCache, missCache sync.Map
	var currentMatchCacheSize, currentMissCacheSize int32

	return func(_ schema.MKey, _ string, tags []string) FilterDecision {
		for _, tag := range tags {
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

			if e.valueRe.MatchString(value) {
				if atomic.LoadInt32(&currentMatchCacheSize) < int32(MatchCacheSize) {
					matchCache.Store(value, struct{}{})
					atomic.AddInt32(&currentMatchCacheSize, 1)
				}
				return Pass
			}

			if atomic.LoadInt32(&currentMissCacheSize) < int32(MatchCacheSize) {
				missCache.Store(value, struct{}{})
				atomic.AddInt32(&currentMissCacheSize, 1)
			}
			continue
		}

		return resultIfTagIsAbsent
	}
}

func (e *expressionMatchTag) StringIntoWriter(writer io.StringWriter) {
	writer.WriteString("__tag=~")
	writer.WriteString(e.value)
}
