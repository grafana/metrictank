package tagquery

import (
	"io"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/grafana/metrictank/schema"
)

type expressionNotMatch struct {
	expressionCommonRe
}

func (e *expressionNotMatch) Equals(other Expression) bool {
	return e.key == other.GetKey() && e.GetOperator() == other.GetOperator() && e.value == other.GetValue()
}

func (e *expressionNotMatch) GetDefaultDecision() FilterDecision {
	// if the pattern matches "" (f.e. "tag!=~.*) then a metric which
	// does not have the tag "tag" at all should not be part of the
	// result set
	// docs: https://graphite.readthedocs.io/en/latest/tags.html
	// > Any tag spec that matches an empty value is considered to
	// > match series that don’t have that tag
	if e.matchesEmpty {
		return Fail
	}
	return Pass
}

func (e *expressionNotMatch) GetOperator() ExpressionOperator {
	return NOT_MATCH
}

func (e *expressionNotMatch) GetOperatorCost() uint32 {
	return 10
}

func (e *expressionNotMatch) RequiresNonEmptyValue() bool {
	return e.matchesEmpty
}

func (e *expressionNotMatch) ResultIsSmallerWhenInverted() bool {
	return true
}

func (e *expressionNotMatch) Matches(value string) bool {
	return !e.valueRe.MatchString(value)
}

func (e *expressionNotMatch) GetMetricDefinitionFilter(_ IdTagLookup) MetricDefinitionFilter {
	if e.key == "name" {
		if e.value == "" {
			// every metric has a name
			return func(_ schema.MKey, _ string, _ []string) FilterDecision { return Pass }
		}

		return func(_ schema.MKey, name string, _ []string) FilterDecision {
			if e.valueRe.MatchString(schema.SanitizeNameAsTagValue(name)) {
				return Fail
			}
			return Pass
		}
	}

	resultIfTagIsAbsent := None
	if !MetaTagSupport {
		resultIfTagIsAbsent = Pass
	}

	prefix := e.key + "="
	var matchCache, missCache sync.Map
	var currentMatchCacheSize, currentMissCacheSize int32
	return func(_ schema.MKey, _ string, tags []string) FilterDecision {
		for _, tag := range tags {
			if !strings.HasPrefix(tag, prefix) {
				continue
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
				if atomic.LoadInt32(&currentMatchCacheSize) < int32(MatchCacheSize) {
					matchCache.Store(value, struct{}{})
					atomic.AddInt32(&currentMatchCacheSize, 1)
				}
				return Fail
			}

			if atomic.LoadInt32(&currentMissCacheSize) < int32(MatchCacheSize) {
				missCache.Store(value, struct{}{})
				atomic.AddInt32(&currentMissCacheSize, 1)
			}
			return Pass
		}

		return resultIfTagIsAbsent
	}
}

func (e *expressionNotMatch) StringIntoWriter(writer io.StringWriter) {
	writer.WriteString(e.key)
	writer.WriteString("!=~")
	writer.WriteString(e.value)
}
