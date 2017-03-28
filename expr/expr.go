package expr

import (
	"fmt"
	"strings"
)

//go:generate stringer -type=exprType
type exprType int

const (
	etName exprType = iota // e.g. a metric query pattern like foo.bar or foo.*.baz
	etFunc
	etConst
	etString
)

type expr struct {
	target    string // the name of a etName, or func name for etFunc. not used for etConst
	etype     exprType
	val       float64 // for etConst
	valStr    string  // for etString, and to store original input for etConst
	args      []*expr // positional
	namedArgs map[string]*expr
	argString string // for etFunc: literal string of how all the args were specified
}

func (e expr) Print(indent int) string {
	space := strings.Repeat(" ", indent)
	switch e.etype {
	case etName:
		return fmt.Sprintf("%sexpr-target %q", space, e.target)
	case etFunc:
		var args string
		for _, a := range e.args {
			args += a.Print(indent+2) + ",\n"
		}
		for k, v := range e.namedArgs {
			args += strings.Repeat(" ", indent+2) + k + "=" + v.Print(0) + ",\n"
		}
		return fmt.Sprintf("%sexpr-func %s(\n%s%s)", space, e.target, args, space)
	case etConst:
		return fmt.Sprintf("%sexpr-const %v", space, e.val)
	case etString:
		return fmt.Sprintf("%sexpr-string %q", space, e.valStr)
	}
	return "HUH-SHOULD-NEVER-HAPPEN"
}
