package memory

import (
	"strings"

	"github.com/grafana/metrictank/idx"
)

type expressionHasTag struct {
	expressionCommon
}

func (e *expressionHasTag) getFilter() tagFilter {
	if e.key == "name" {
		return func(def *idx.Archive) filterDecision { return pass }
	}
	return func(def *idx.Archive) filterDecision {
		matchPrefix := e.getKey() + "="
		for _, tag := range def.Tags {
			if strings.HasPrefix(tag, matchPrefix) {
				return pass
			}
		}
		return none
	}
}

func (e *expressionHasTag) hasRe() bool {
	return false
}

func (e *expressionHasTag) getMatcher() tagStringMatcher {
	// in the case of the opHasTag operator the value just doesn't matter at all
	return func(checkValue string) bool {
		return true
	}
}

func (e *expressionHasTag) matchesTag() bool {
	return true
}

func (e *expressionHasTag) getDefaultDecision() filterDecision {
	return fail
}

func (e *expressionHasTag) stringIntoBuilder(builder *strings.Builder) {
	builder.WriteString(e.key)
	builder.WriteString("!=")
}

func (e *expressionHasTag) getMetaRecords(mti metaTagIndex) []uint32 {
	return nil
}
