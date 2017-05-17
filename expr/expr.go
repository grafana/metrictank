package expr

import (
	"fmt"
	"regexp"
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

// consumeBasicArg verifies that the argument at given pos matches the expected arg
// it's up to the caller to assure that given pos is valid before calling.
// if arg allows for multiple arguments, pos is advanced to cover all accepted arguments.
// if the arg is a "basic" arg (meaning not a series, seriesList or seriesLists) the
// appropriate value(s) will be assigned to exp.val
// for non-basic args, see consumeSeriesArg which should be called after deducing the required from/to.
// the returned pos is always the index where the next argument should be.
func (e expr) consumeBasicArg(pos int, exp Arg) (int, error) {
	got := e.args[pos]
	switch v := exp.(type) {
	case ArgSeries, ArgSeriesList:
		if got.etype != etName && got.etype != etFunc {
			return 0, ErrBadArgumentStr{"func or name", string(got.etype)}
		}
	case ArgSeriesLists:
		if got.etype != etName && got.etype != etFunc {
			return 0, ErrBadArgumentStr{"func or name", string(got.etype)}
		}
		// special case! consume all subsequent args (if any) in args that will also yield a seriesList
		for len(e.args) > pos+1 && (e.args[pos+1].etype == etName || e.args[pos+1].etype == etFunc) {
			pos += 1
		}
	case ArgInt:
		if got.etype != etInt {
			return 0, ErrBadArgumentStr{"int", string(got.etype)}
		}
		for _, va := range v.validator {
			if err := va(got); err != nil {
				return 0, fmt.Errorf("%s: %s", v.key, err.Error())
			}
		}
		*v.val = got.int
	case ArgInts:
		if got.etype != etInt {
			return 0, ErrBadArgumentStr{"int", string(got.etype)}
		}
		*v.val = append(*v.val, got.int)
		// special case! consume all subsequent args (if any) in args that will also yield an integer
		for len(e.args) > pos+1 && e.args[pos+1].etype == etInt {
			pos += 1
			for _, va := range v.validator {
				if err := va(e.args[pos]); err != nil {
					return 0, fmt.Errorf("%s: %s", v.key, err.Error())
				}
			}
			*v.val = append(*v.val, e.args[pos].int)
		}
	case ArgFloat:
		// integer is also a valid float, just happened to have no decimals
		if got.etype != etFloat && got.etype != etInt {
			return 0, ErrBadArgumentStr{"float", string(got.etype)}
		}
		for _, va := range v.validator {
			if err := va(got); err != nil {
				return 0, fmt.Errorf("%s: %s", v.key, err.Error())
			}
		}
		if got.etype == etInt {
			*v.val = float64(got.int)
		} else {
			*v.val = got.float
		}
	case ArgString:
		if got.etype != etString {
			return 0, ErrBadArgumentStr{"string", string(got.etype)}
		}
		for _, va := range v.validator {
			if err := va(got); err != nil {
				return 0, fmt.Errorf("%s: %s", v.key, err.Error())
			}
		}
		*v.val = got.str
	case ArgRegex:
		if got.etype != etString {
			return 0, ErrBadArgumentStr{"string (regex)", string(got.etype)}
		}
		for _, va := range v.validator {
			if err := va(got); err != nil {
				return 0, fmt.Errorf("%s: %s", v.key, err.Error())
			}
		}
		re, err := regexp.Compile(got.str)
		if err != nil {
			return 0, err
		}
		*v.val = re
	case ArgBool:
		if got.etype != etBool {
			return 0, ErrBadArgumentStr{"string", string(got.etype)}
		}
		*v.val = got.bool
	default:
		return 0, fmt.Errorf("unsupported type %T for consumeBasicArg", exp)
	}
	pos += 1
	return pos, nil
}

// consumeSeriesArg verifies that the argument at given pos matches the expected arg
// it's up to the caller to assure that given pos is valid before calling.
// if arg allows for multiple arguments, pos is advanced to cover all accepted arguments.
// if the arg is a "basic", no value is saved (it's up to consumeBasicArg to do that)
// but for non-basic args (meaning a series, seriesList or seriesLists) the
// appropriate value(s) will be assigned to exp.val
// the returned pos is always the index where the next argument should be.
func (e expr) consumeSeriesArg(pos int, exp Arg, context Context, stable bool, reqs []Req) (int, []Req, error) {
	got := e.args[pos]
	var err error
	var fn GraphiteFunc
	switch v := exp.(type) {
	case ArgSeries:
		if got.etype != etName && got.etype != etFunc {
			return 0, nil, ErrBadArgumentStr{"func or name", string(got.etype)}
		}
		fn, reqs, err = newplan(got, context, stable, reqs)
		if err != nil {
			return 0, nil, err
		}
		*v.val = fn
	case ArgSeriesList:
		if got.etype != etName && got.etype != etFunc {
			return 0, nil, ErrBadArgumentStr{"func or name", string(got.etype)}
		}
		fn, reqs, err = newplan(got, context, stable, reqs)
		if err != nil {
			return 0, nil, err
		}
		*v.val = fn
	case ArgSeriesLists:
		if got.etype != etName && got.etype != etFunc {
			return 0, nil, ErrBadArgumentStr{"func or name", string(got.etype)}
		}
		fn, reqs, err = newplan(got, context, stable, reqs)
		if err != nil {
			return 0, nil, err
		}
		*v.val = append(*v.val, fn)
		// special case! consume all subsequent args (if any) in args that will also yield a seriesList
		for len(e.args) > pos+1 && (e.args[pos+1].etype == etName || e.args[pos+1].etype == etFunc) {
			pos += 1
			fn, reqs, err = newplan(e.args[pos], context, stable, reqs)
			if err != nil {
				return 0, nil, err
			}
			*v.val = append(*v.val, fn)
		}
	default:
		return 0, nil, fmt.Errorf("unsupported type %T for consumeSeriesArg", exp)
	}
	pos += 1
	return pos, reqs, nil
}

// consumeKwarg consumes the kwarg (by key k) and verifies it
// if the specified argument is valid, it is saved in exp.val
// where exp is the arg specified by the function that has the given key
func (e expr) consumeKwarg(key string, optArgs []Arg) error {
	var found bool
	var exp Arg
	for _, exp = range optArgs {
		if exp.Key() == key {
			found = true
			break
		}
	}
	if !found {
		return ErrUnknownKwarg{key}
	}
	got := e.namedArgs[key]
	switch v := exp.(type) {
	case ArgInt:
		if got.etype != etInt {
			return ErrBadKwarg{key, exp, got.etype}
		}
		*v.val = got.int
	case ArgFloat:
		switch got.etype {
		case etInt:
			// integer is also a valid float, just happened to have no decimals
			*v.val = float64(got.int)
		case etFloat:
			*v.val = got.float
		default:
			return ErrBadKwarg{key, exp, got.etype}
		}
	case ArgString:
		if got.etype != etString {
			return ErrBadKwarg{key, exp, got.etype}
		}
		*v.val = got.str
	case ArgBool:
		if got.etype != etBool {
			return ErrBadKwarg{key, exp, got.etype}
		}
		*v.val = got.bool
	default:
		return fmt.Errorf("unsupported type %T for consumeKwarg", exp)
	}
	return nil
}
