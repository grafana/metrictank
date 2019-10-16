package tagquery

import (
	"io"
	"strings"

	"github.com/grafana/metrictank/schema"
)

type expressionEqual struct {
	expressionCommon
}

func (e *expressionEqual) Equals(other Expression) bool {
	return e.key == other.GetKey() && e.GetOperator() == other.GetOperator() && e.value == other.GetValue()
}

func (e *expressionEqual) GetDefaultDecision() FilterDecision {
	return Fail
}

func (e *expressionEqual) GetOperator() ExpressionOperator {
	return EQUAL
}

func (e *expressionEqual) GetOperatorCost() uint32 {
	return 1
}

func (e *expressionEqual) Matches(value string) bool {
	return value == e.value
}

func (e *expressionEqual) MatchesExactly() bool {
	return true
}

func (e *expressionEqual) GetMetricDefinitionFilter(lookup IdTagLookup) MetricDefinitionFilter {
	if e.key == "name" {
		if e.value == "" {
			// every metric has a name, the value will never be empty
			return func(_ schema.MKey, _ string, _ []string) FilterDecision { return Fail }
		}

		return func(_ schema.MKey, name string, _ []string) FilterDecision {
			if name == e.value {
				return Pass
			}
			return Fail
		}
	}

	if !MetaTagSupport {
		return func(id schema.MKey, _ string, _ []string) FilterDecision {
			if lookup(id, e.key, e.value) {
				return Pass
			}
			return Fail
		}
	}

	prefix := e.key + "="
	return func(id schema.MKey, _ string, tags []string) FilterDecision {
		if lookup(id, e.key, e.value) {
			return Pass
		}

		for _, tag := range tags {
			// the tag is set, but it has a different value,
			// no need to keep looking at other indexes
			if strings.HasPrefix(tag, prefix) {
				return Fail
			}
		}

		return None
	}
}

func (e *expressionEqual) StringIntoWriter(writer io.StringWriter) {
	writer.WriteString(e.key)
	writer.WriteString("=")
	writer.WriteString(e.value)
}
