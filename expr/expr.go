package expr

import (
	"fmt"
	"strings"
)

//go:generate stringer -type=exprType
type exprType int

// the following types let the parser express the type it parsed from the input targets
const (
	etName   exprType = iota // a string without quotes, e.g. metric.name, metric.*.query.patt* or special values like None which some functions expect
	etBool                   // True or False
	etFunc                   // a function call like movingAverage(foo, bar)
	etInt                    // any number with no decimal numbers, parsed as a float64 value
	etFloat                  // any number with decimals, parsed as a float64 value
	etString                 // anything that was between '' or ""
)

// expr represents a parsed expression
type expr struct {
	etype     exprType
	float     float64          // for etFloat
	int       int64            // for etInt
	str       string           // for etName, etFunc (func name), etString, etBool, etInt and etFloat (unparsed input value)
	bool      bool             // for etBool
	args      []*expr          // for etFunc: positional args which itself are expressions
	namedArgs map[string]*expr // for etFunc: named args which itself are expressions
	argsStr   string           // for etFunc: literal string of how all the args were specified
}

func (e expr) Print(indent int) string {
	space := strings.Repeat(" ", indent)
	switch e.etype {
	case etName:
		return fmt.Sprintf("%sexpr-target %q", space, e.str)
	case etFunc:
		var args string
		for _, a := range e.args {
			args += a.Print(indent+2) + ",\n"
		}
		for k, v := range e.namedArgs {
			args += strings.Repeat(" ", indent+2) + k + "=" + v.Print(0) + ",\n"
		}
		return fmt.Sprintf("%sexpr-func %s(\n%s%s)", space, e.str, args, space)
	case etFloat:
		return fmt.Sprintf("%sexpr-float %v", space, e.float)
	case etInt:
		return fmt.Sprintf("%sexpr-int %v", space, e.int)
	case etString:
		return fmt.Sprintf("%sexpr-string %q", space, e.str)
	}
	return "HUH-SHOULD-NEVER-HAPPEN"
}
