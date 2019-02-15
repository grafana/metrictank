package memory

import (
	"strings"

	"github.com/grafana/metrictank/idx"
)

type expressionNotHasTag struct {
	expressionCommon
}

func (e *expressionNotHasTag) getFilter() tagFilter {
	if e.key == "name" {
		return func(def *idx.Archive) filterDecision { return fail }
	}
	return func(def *idx.Archive) filterDecision {
		matchPrefix := e.getKey() + "="
		for _, tag := range def.Tags {
			if strings.HasPrefix(tag, matchPrefix) {
				return fail
			}
		}
		return none
	}
}

func (e *expressionNotHasTag) hasRe() bool {
	return false
}

func (e *expressionNotHasTag) getMatcher() tagStringMatcher {
	// in the case of the opNotHasTag operator the value just doesn't matter at all
	return func(checkValue string) bool {
		return false
	}
}

func (e *expressionNotHasTag) matchesTag() bool {
	return true
}

func (e *expressionNotHasTag) getDefaultDecision() filterDecision {
	return pass
}

func (e *expressionNotHasTag) stringIntoBuilder(builder *strings.Builder) {
	builder.WriteString(e.key)
	builder.WriteString("=")
}

func (e *expressionNotHasTag) getMetaRecords(mti metaTagIndex) []uint32 {
	return nil
}
