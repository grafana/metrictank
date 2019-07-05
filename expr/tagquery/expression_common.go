package tagquery

import "regexp"

type expressionCommon struct {
	key   string
	value string
}

func (e *expressionCommon) GetKey() string {
	return e.key
}

func (e *expressionCommon) GetValue() string {
	return e.value
}

func (e *expressionCommon) RequiresNonEmptyValue() bool {
	// by default assume true, unless a concrete type overrides this method
	return true
}

func (e *expressionCommon) OperatesOnTag() bool {
	// by default assume false, unless a concrete type overrides this method
	return false
}

func (e *expressionCommon) HasRe() bool {
	// by default assume false, unless a concrete type overrides this method
	return false
}

// expressionCommonRe is an extended version of expressionCommon with additional
// properties for operators that use regular expressions
type expressionCommonRe struct {
	expressionCommon
	valueRe      *regexp.Regexp
	matchesEmpty bool
}
