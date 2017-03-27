package expr

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"
)

var (
	ErrMissingArg         = errors.New("argument missing")
	ErrTooManyArg         = errors.New("too many arguments")
	ErrMissingTimeseries  = errors.New("missing time series argument")
	ErrWildcardNotAllowed = errors.New("found wildcard where series expected")
)

type ErrBadArgument struct {
	exp reflect.Type
	got reflect.Type
}

func (e ErrBadArgument) Error() string {
	return fmt.Sprintf("argument bad type. expected %s - got %s", e.exp, e.got)
}

type ErrBadArgumentStr struct {
	exp string
	got string
}

func (e ErrBadArgumentStr) Error() string {
	return fmt.Sprintf("argument bad type. expected %s - got %s", e.exp, e.got)
}

type ErrUnknownFunction string

func (e ErrUnknownFunction) Error() string {
	return fmt.Sprintf("unknown function %q", string(e))
}

type MetricRequest struct {
	Metric string
	From   int32
	Until  int32
}

func ParseMany(targets []string) ([]*expr, error) {
	var out []*expr
	for _, target := range targets {
		e, leftover, err := Parse(target)
		if err != nil {
			return nil, err
		}
		if leftover != "" {
			panic(fmt.Sprintf("failed to parse %q fully. got leftover %q", target, leftover))
		}
		out = append(out, e)
	}
	return out, nil
}

func Parse(e string) (*expr, string, error) {
	// skip whitespace
	for len(e) > 1 && e[0] == ' ' {
		e = e[1:]
	}

	if len(e) == 0 {
		return nil, "", ErrMissingExpr
	}

	if '0' <= e[0] && e[0] <= '9' || e[0] == '-' || e[0] == '+' {
		val, valStr, e, err := parseConst(e)
		return &expr{val: val, valStr: valStr, etype: etConst}, e, err
	}

	if e[0] == '\'' || e[0] == '"' {
		val, e, err := parseString(e)
		return &expr{valStr: val, etype: etString}, e, err
	}

	name, e := parseName(e)

	if name == "" {
		return nil, e, ErrMissingArg
	}

	if e != "" && e[0] == '(' {
		exp := &expr{target: name, etype: etFunc}

		argString, posArgs, namedArgs, e, err := parseArgList(e)
		exp.argString = argString
		exp.args = posArgs
		exp.namedArgs = namedArgs

		return exp, e, err
	}

	return &expr{target: name}, e, nil
}

var (
	// ErrMissingExpr is a parse error returned when an expression is missing.
	ErrMissingExpr = errors.New("missing expression")
	// ErrMissingComma is a parse error returned when an expression is missing a comma.
	ErrMissingComma = errors.New("missing comma")
	// ErrMissingQuote is a parse error returned when an expression is missing a quote.
	ErrMissingQuote = errors.New("missing quote")
	// ErrUnexpectedCharacter is a parse error returned when an expression contains an unexpected character.
	ErrUnexpectedCharacter = errors.New("unexpected character")
)

func parseArgList(e string) (string, []*expr, map[string]*expr, string, error) {

	var (
		posArgs   []*expr
		namedArgs map[string]*expr
	)

	if e[0] != '(' {
		panic("arg list should start with paren")
	}

	argString := e[1:]

	e = e[1:]

	for {
		var arg *expr
		var err error
		arg, e, err = Parse(e)
		if err != nil {
			return "", nil, nil, e, err
		}

		if e == "" {
			return "", nil, nil, "", ErrMissingComma
		}

		// we now know we're parsing a key-value pair
		if arg.etype == etName && e[0] == '=' {
			e = e[1:]
			argCont, eCont, errCont := Parse(e)
			if errCont != nil {
				return "", nil, nil, eCont, errCont
			}

			if eCont == "" {
				return "", nil, nil, "", ErrMissingComma
			}

			if argCont.etype != etConst && argCont.etype != etName && argCont.etype != etString {
				return "", nil, nil, eCont, ErrBadArgumentStr{"const, name or string", string(argCont.etype)}
			}

			if namedArgs == nil {
				namedArgs = make(map[string]*expr)
			}

			namedArgs[arg.target] = &expr{
				etype:  argCont.etype,
				val:    argCont.val,
				valStr: argCont.valStr,
				target: argCont.target,
			}

			e = eCont
		} else {
			posArgs = append(posArgs, arg)
		}

		// after the argument, trim any trailing spaces
		for len(e) > 0 && e[0] == ' ' {
			e = e[1:]
		}

		if e[0] == ')' {
			return argString[:len(argString)-len(e)], posArgs, namedArgs, e[1:], nil
		}

		if e[0] != ',' && e[0] != ' ' {
			return "", nil, nil, "", ErrUnexpectedCharacter
		}

		e = e[1:]
	}
}

func isNameChar(r byte) bool {
	return false ||
		'a' <= r && r <= 'z' ||
		'A' <= r && r <= 'Z' ||
		'0' <= r && r <= '9' ||
		r == '.' || r == '_' || r == '-' || r == '*' || r == '?' || r == ':' ||
		r == '[' || r == ']' ||
		r == '<' || r == '>'
}

func isDigit(r byte) bool {
	return '0' <= r && r <= '9'
}

func parseConst(s string) (float64, string, string, error) {

	var i int
	// All valid characters for a floating-point constant
	// Just slurp them all in and let ParseFloat sort 'em out
	for i < len(s) && (isDigit(s[i]) || s[i] == '.' || s[i] == '+' || s[i] == '-' || s[i] == 'e' || s[i] == 'E') {
		i++
	}

	v, err := strconv.ParseFloat(s[:i], 64)
	if err != nil {
		return 0, "", "", err
	}

	return v, s[:i], s[i:], err
}

func parseName(s string) (string, string) {

	var i int

FOR:
	for braces := 0; i < len(s); i++ {

		if isNameChar(s[i]) {
			continue
		}

		switch s[i] {
		case '{':
			braces++
		case '}':
			if braces == 0 {
				break FOR

			}
			braces--
		case ',':
			if braces == 0 {
				break FOR
			}
		default:
			break FOR
		}

	}

	if i == len(s) {
		return s, ""
	}

	return s[:i], s[i:]
}

func parseString(s string) (string, string, error) {

	if s[0] != '\'' && s[0] != '"' {
		panic("string should start with open quote")
	}

	match := s[0]

	s = s[1:]

	var i int
	for i < len(s) && s[i] != match {
		i++
	}

	if i == len(s) {
		return "", "", ErrMissingQuote

	}
	//fmt.Println("> string", s[:i])

	return s[:i], s[i+1:], nil
}

func getStringArg(e *expr, n int) (string, error) {
	if len(e.args) <= n {
		return "", ErrMissingArg
	}

	return doGetStringArg(e.args[n])
}

func getStringArgDefault(e *expr, n int, s string) (string, error) {
	if len(e.args) <= n {
		return s, nil
	}

	return doGetStringArg(e.args[n])
}

func getStringNamedOrPosArgDefault(e *expr, k string, n int, s string) (string, error) {
	if a := getNamedArg(e, k); a != nil {
		return doGetStringArg(a)
	}

	return getStringArgDefault(e, n, s)
}

func doGetStringArg(e *expr) (string, error) {
	if e.etype != etString {
		return "", ErrBadArgumentStr{"string", string(e.etype)}
	}

	return e.valStr, nil
}

/*
func getIntervalArg(e *expr, n int, defaultSign int) (int32, error) {
	if len(e.args) <= n {
		return 0, ErrMissingArg
	}

	if e.args[n].etype != etString {
		return 0, ErrBadArgumentStr{"string", string(e.etype)}
	}

	seconds, err := IntervalString(e.args[n].valStr, defaultSign)
	if err != nil {
		return 0, ErrBadArgumentStr{"const", string(e.etype)}
	}

	return seconds, nil
}
*/

func getFloatArg(e *expr, n int) (float64, error) {
	if len(e.args) <= n {
		return 0, ErrMissingArg
	}

	return doGetFloatArg(e.args[n])
}

func getFloatArgDefault(e *expr, n int, v float64) (float64, error) {
	if len(e.args) <= n {
		return v, nil
	}

	return doGetFloatArg(e.args[n])
}

func getFloatNamedOrPosArgDefault(e *expr, k string, n int, v float64) (float64, error) {
	if a := getNamedArg(e, k); a != nil {
		return doGetFloatArg(a)
	}

	return getFloatArgDefault(e, n, v)
}

func doGetFloatArg(e *expr) (float64, error) {
	if e.etype != etConst {
		return 0, ErrBadArgumentStr{"const", string(e.etype)}
	}

	return e.val, nil
}

func getIntArg(e *expr, n int) (int, error) {
	if len(e.args) <= n {
		return 0, ErrMissingArg
	}

	return doGetIntArg(e.args[n])
}

func getIntArgs(e *expr, n int) ([]int, error) {

	if len(e.args) <= n {
		return nil, ErrMissingArg
	}

	var ints []int

	for i := n; i < len(e.args); i++ {
		a, err := getIntArg(e, i)
		if err != nil {
			return nil, err
		}
		ints = append(ints, a)
	}

	return ints, nil
}

func getIntArgDefault(e *expr, n int, d int) (int, error) {
	if len(e.args) <= n {
		return d, nil
	}

	return doGetIntArg(e.args[n])
}

func getIntNamedOrPosArgDefault(e *expr, k string, n int, d int) (int, error) {
	if a := getNamedArg(e, k); a != nil {
		return doGetIntArg(a)
	}

	return getIntArgDefault(e, n, d)
}

func doGetIntArg(e *expr) (int, error) {
	if e.etype != etConst {
		return 0, ErrBadArgumentStr{"const", string(e.etype)}
	}

	return int(e.val), nil
}

func getBoolNamedOrPosArgDefault(e *expr, k string, n int, b bool) (bool, error) {
	if a := getNamedArg(e, k); a != nil {
		return doGetBoolArg(a)
	}

	return getBoolArgDefault(e, n, b)
}

func getBoolArgDefault(e *expr, n int, b bool) (bool, error) {
	if len(e.args) <= n {
		return b, nil
	}

	return doGetBoolArg(e.args[n])
}

func doGetBoolArg(e *expr) (bool, error) {
	if e.etype != etName {
		return false, ErrBadArgumentStr{"const", string(e.etype)}
	}

	// names go into 'target'
	switch e.target {
	case "False", "false":
		return false, nil
	case "True", "true":
		return true, nil
	}

	return false, ErrBadArgumentStr{"const", string(e.etype)}
}

/*
func getSeriesArg(arg *expr, from, until int32, values map[MetricRequest][]*MetricData) ([]*MetricData, error) {
	if arg.etype != etName && arg.etype != etFunc {
		return nil, ErrMissingTimeseries
	}

	a, _ := EvalExpr(arg, from, until, values)

	if len(a) == 0 {
		return nil, ErrSeriesDoesNotExist
	}

	return a, nil
}

func getSeriesArgs(e []*expr, from, until int32, values map[MetricRequest][]*MetricData) ([]*MetricData, error) {

	var args []*MetricData

	for _, arg := range e {
		a, err := getSeriesArg(arg, from, until, values)
		if err != nil {
			return nil, err
		}
		args = append(args, a...)
	}

	if len(args) == 0 {
		return nil, ErrSeriesDoesNotExist
	}

	return args, nil
}
*/

func getNamedArg(e *expr, name string) *expr {
	if a, ok := e.namedArgs[name]; ok {
		return a
	}

	return nil
}
