package expr

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/grafana/metrictank/errors"
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
	args      []*expr          // for etFunc: positional args which itself are expressions (including piped in args)
	namedArgs map[string]*expr // for etFunc: named args which itself are expressions
	argsStr   string           // for etFunc: literal string of how all the args were specified (excluding piped in args)
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
	if got.etype == etName && got.str == "None" {
		if !exp.Optional() {
			return 0, ErrMissingArg
		} else {
			pos++
			return pos, nil
		}
	}
	switch v := exp.(type) {
	case ArgBool:
		if got.etype == etBool {
			*v.val = got.bool
			break
		}
		if got.etype == etString {
			if val, ok := strToBool(got.str); ok {
				*v.val = val
				break
			}
		}
		return 0, ErrBadArgumentStr{"boolean", got.etype.String()}
	case ArgIn:
		for _, a := range v.args {

			p, err := e.consumeBasicArg(pos, a)
			if err == nil {
				return p, err
			}
		}
		expStr := []string{}
		for _, a := range v.args {
			expStr = append(expStr, fmt.Sprintf("%T", a))
		}
		return 0, ErrBadArgumentStr{strings.Join(expStr, ","), got.etype.String()}
	case ArgInt:
		switch got.etype {
		case etInt:
			for _, va := range v.validator {
				if err := va(got); err != nil {
					return 0, generateValidatorError(v.key, err)
				}
			}
			*v.val = got.int
		case etString:
			// allow "2", '5', etc. some people put quotes around their ints.
			i, ok := strToInt(got.str)
			if !ok {
				return 0, ErrBadArgumentStr{"int", got.etype.String()}
			}
			gotFake := expr{
				str:   got.str,
				int:   int64(i),
				etype: etInt,
			}
			for _, va := range v.validator {
				if err := va(&gotFake); err != nil {
					return 0, generateValidatorError(v.key, err)
				}
			}
			*v.val = int64(i)
		default:
			return 0, ErrBadArgumentStr{"int", got.etype.String()}
		}
	case ArgInts:
		// consume all args (if any) in args that will yield an integer
		for ; pos < len(e.args) && e.args[pos].etype == etInt; pos++ {
			for _, va := range v.validator {
				if err := va(e.args[pos]); err != nil {
					return 0, generateValidatorError(v.key, err)
				}
			}
			*v.val = append(*v.val, e.args[pos].int)
		}
		return pos, nil
	case ArgFloat:
		switch got.etype {
		// integer is also a valid float, just happened to have no decimals
		case etInt, etFloat:
			for _, va := range v.validator {
				if err := va(got); err != nil {
					return 0, generateValidatorError(v.key, err)
				}
			}
			if got.etype == etInt {
				*v.val = float64(got.int)
			} else {
				*v.val = got.float
			}
		case etString:
			// allow "2.5", '5.4', etc. some people put quotes around their floats.
			f, ok := strToFloat(got.str)
			if !ok {
				return 0, ErrBadArgumentStr{"float", got.etype.String()}
			}
			gotFake := expr{
				str:   got.str,
				float: f,
				etype: etFloat,
			}
			for _, va := range v.validator {
				if err := va(&gotFake); err != nil {
					return 0, generateValidatorError(v.key, err)
				}
			}
			*v.val = f
		default:
			return 0, ErrBadArgumentStr{"float", got.etype.String()}
		}

	case ArgRegex:
		if got.etype != etString {
			return 0, ErrBadArgumentStr{"string (regex)", got.etype.String()}
		}
		for _, va := range v.validator {
			if err := va(got); err != nil {
				return 0, generateValidatorError(v.key, err)
			}
		}
		re, err := regexp.Compile(got.str)
		if err != nil {
			return 0, ErrBadRegex{err}
		}
		*v.val = re
	case ArgSeries, ArgSeriesList:
		if got.etype != etName && got.etype != etFunc {
			return 0, ErrBadArgumentStr{"func or name", got.etype.String()}
		}
	case ArgSeriesLists:
		if got.etype != etName && got.etype != etFunc {
			return 0, ErrBadArgumentStr{"func or name", got.etype.String()}
		}
		// special case! consume all subsequent args (if any) in args that will also yield a seriesList
		for len(e.args) > pos+1 && (e.args[pos+1].etype == etName || e.args[pos+1].etype == etFunc) {
			pos++
		}
	case ArgString:
		if got.etype != etString {
			return 0, ErrBadArgumentStr{"string", got.etype.String()}
		}
		for _, va := range v.validator {
			if err := va(got); err != nil {
				return 0, generateValidatorError(v.key, err)
			}
		}
		*v.val = got.str
	case ArgStrings:
		// consume all args (if any) in args that will yield a string
		for ; pos < len(e.args) && e.args[pos].etype == etString; pos++ {
			for _, va := range v.validator {
				if err := va(e.args[pos]); err != nil {
					return 0, generateValidatorError(v.key, err)
				}
			}
			*v.val = append(*v.val, e.args[pos].str)
		}
		return pos, nil
	case ArgStringsOrInts:
		// consume all args (if any) in args that will yield a string or int
		for ; len(e.args) > pos && (e.args[pos].etype == etString || e.args[pos].etype == etInt); pos++ {
			for _, va := range v.validator {
				if err := va(e.args[pos]); err != nil {
					return 0, generateValidatorError(v.key, err)
				}
			}
			*v.val = append(*v.val, *e.args[pos])
		}
		return pos, nil
	case ArgStringOrInt:
		if got.etype != etString && got.etype != etInt {
			return 0, ErrBadArgumentStr{"string or int", got.etype.String()}
		}
		for _, va := range v.validator {
			if err := va(got); err != nil {
				return 0, generateValidatorError(v.key, err)
			}
		}
		*v.val = *got
	case ArgQuotelessString:
		if got.etype != etName {
			return 0, ErrBadArgumentStr{"quoteless string", got.etype.String()}
		}
		for _, va := range v.validator {
			if err := va(got); err != nil {
				return 0, generateValidatorError(v.key, err)
			}
		}
		*v.val = got.str
	default:
		return 0, errors.NewBadRequestf("unsupported type %T for consumeBasicArg", exp)
	}
	pos++
	return pos, nil
}

func generateValidatorError(key string, err error) error {
	if len(key) == 0 {
		return errors.NewBadRequest(err.Error())
	}
	return errors.NewBadRequestf("%s: %s", key, err.Error())
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
	if got.etype == etName && got.str == "None" {
		pos++
		return pos, reqs, nil
	}

Switch:
	switch v := exp.(type) {
	case ArgIn:
		if got.etype == etName || got.etype == etFunc {
			for _, a := range v.args {
				switch v := a.(type) {
				case ArgSeries, ArgSeriesList, ArgSeriesLists:
					p, reqs, err := e.consumeSeriesArg(pos, v, context, stable, reqs)
					if err != nil {
						return 0, nil, err
					}
					return p, reqs, err
				case ArgQuotelessString:
					break Switch
				default:
				}
			}
			expStr := []string{}
			for _, a := range v.args {
				expStr = append(expStr, fmt.Sprintf("%T", a))
			}
			return 0, nil, ErrBadArgumentStr{strings.Join(expStr, ","), got.etype.String()}
		}
	case ArgSeries:
		if got.etype != etName && got.etype != etFunc {
			return 0, nil, ErrBadArgumentStr{"func or name", got.etype.String()}
		}
		fn, reqs, err = newplan(got, context, stable, reqs)
		if err != nil {
			return 0, nil, err
		}
		*v.val = fn
	case ArgSeriesList:
		if got.etype != etName && got.etype != etFunc {
			return 0, nil, ErrBadArgumentStr{"func or name", got.etype.String()}
		}
		fn, reqs, err = newplan(got, context, stable, reqs)
		if err != nil {
			return 0, nil, err
		}
		*v.val = fn
	case ArgSeriesLists:
		if got.etype != etName && got.etype != etFunc {
			return 0, nil, ErrBadArgumentStr{"func or name", got.etype.String()}
		}
		fn, reqs, err = newplan(got, context, stable, reqs)
		if err != nil {
			return 0, nil, err
		}
		*v.val = append(*v.val, fn)
		// special case! consume all subsequent args (if any) in args that will also yield a seriesList
		for len(e.args) > pos+1 && (e.args[pos+1].etype == etName || e.args[pos+1].etype == etFunc) {
			pos++
			fn, reqs, err = newplan(e.args[pos], context, stable, reqs)
			if err != nil {
				return 0, nil, err
			}
			*v.val = append(*v.val, fn)
		}
	default:
		return 0, nil, errors.NewBadRequestf("unsupported type %T for consumeSeriesArg", exp)
	}
	pos++
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
	case ArgBool:
		if got.etype == etBool {
			*v.val = got.bool
			break
		}
		if got.etype == etString {
			if val, ok := strToBool(got.str); ok {
				*v.val = val
				break
			}
		}
		return ErrBadKwarg{key, exp, got.etype}
	case ArgIn:
		for _, a := range v.args {
			// interesting little trick here.. when using ArgIn you only have to set the key on ArgIn,
			// not for every individual sub-arg so to make sure we pass the key matching requirement,
			// we just call consumeKwarg with whatever the key is set to (typically "")
			// and set up optArgs and namedArgs such that it will work
			subE := expr{
				namedArgs: map[string]*expr{a.Key(): got},
			}
			err := subE.consumeKwarg(a.Key(), []Arg{a})
			if err == nil {
				return err
			}
		}
		expStr := []string{}
		for _, a := range v.args {
			expStr = append(expStr, fmt.Sprintf("%T", a))
		}
		return ErrBadArgumentStr{strings.Join(expStr, ","), got.etype.String()}
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
	case ArgSeries, ArgSeriesList, ArgSeriesLists:
		if got.etype != etName && got.etype != etFunc {
			return ErrBadArgumentStr{"func or name", got.etype.String()}
		}
		// TODO consume series arg
	case ArgString:
		if got.etype != etString {
			return ErrBadKwarg{key, exp, got.etype}
		}
		*v.val = got.str
	default:
		return errors.NewBadRequestf("unsupported type %T for consumeKwarg", exp)
	}
	return nil
}
