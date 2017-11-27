package expr

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

var (
	ErrMissingArg          = errors.New("argument missing")
	ErrTooManyArg          = errors.New("too many arguments")
	ErrMissingTimeseries   = errors.New("missing time series argument")
	ErrWildcardNotAllowed  = errors.New("found wildcard where series expected")
	ErrMissingExpr         = errors.New("missing expression")
	ErrMissingComma        = errors.New("missing comma")
	ErrMissingQuote        = errors.New("missing quote")
	ErrUnexpectedCharacter = errors.New("unexpected character")
	ErrIllegalCharacter    = errors.New("illegal character for function name")
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

type ErrUnknownKwarg struct {
	key string
}

func (e ErrUnknownKwarg) Error() string {
	return fmt.Sprintf("unknown keyword argument %q", e)
}

type ErrBadKwarg struct {
	key string
	exp Arg
	got exprType
}

func (e ErrBadKwarg) Error() string {
	return fmt.Sprintf("keyword argument %q bad type. expected %T - got %s", e.key, e.exp, e.got)
}

type ErrKwargSpecifiedTwice struct {
	key string
}

func (e ErrKwargSpecifiedTwice) Error() string {
	return fmt.Sprintf("keyword argument %q specified twice", e.key)
}

type MetricRequest struct {
	Metric string
	From   int32
	Until  int32
}

// ParseMany parses a slice of strings into a slice of expressions (recursively)
// not included: validation that requested functions exist, correct args are passed, etc.
func ParseMany(targets []string) ([]*expr, error) {
	var out []*expr
	for _, target := range targets {
		e, leftover, err := Parse(target)
		if err != nil {
			return nil, err
		}
		if leftover != "" {
			return nil, fmt.Errorf("failed to parse %q fully. got leftover %q", target, leftover)
		}
		out = append(out, e)
	}
	return out, nil
}

// Parses an expression string and turns it into an expression
// also returns any leftover data that could not be parsed
func Parse(e string) (*expr, string, error) {
	// skip whitespace
	for len(e) > 1 && e[0] == ' ' {
		e = e[1:]
	}

	if len(e) == 0 {
		return nil, "", ErrMissingExpr
	}

	if '0' <= e[0] && e[0] <= '9' || e[0] == '-' || e[0] == '+' {
		return parseConst(e)
	}

	if strings.HasPrefix(e, "True") || strings.HasPrefix(e, "true") {
		return &expr{bool: true, str: e[:4], etype: etBool}, e[4:], nil
	}

	if strings.HasPrefix(e, "False") || strings.HasPrefix(e, "false") {
		return &expr{bool: false, str: e[:5], etype: etBool}, e[5:], nil
	}

	if e[0] == '\'' || e[0] == '"' {
		val, e, err := parseString(e)
		return &expr{str: val, etype: etString}, e, err
	}

	name, e := parseName(e)

	if name == "" {
		return nil, e, ErrMissingArg
	}

	if e != "" && e[0] == '(' {
		for i := range name {
			if !isFnChar(name[i]) {
				return nil, "", ErrIllegalCharacter
			}
		}

		exp := &expr{str: name, etype: etFunc}

		ArgString, posArgs, namedArgs, e, err := parseArgList(e)
		exp.argsStr = ArgString
		exp.args = posArgs
		exp.namedArgs = namedArgs

		return exp, e, err
	}

	return &expr{str: name, etype: etName}, e, nil
}

// caller must assure s starts with opening paren
func parseArgList(e string) (string, []*expr, map[string]*expr, string, error) {

	var (
		posArgs   []*expr
		namedArgs map[string]*expr
	)

	if e[0] != '(' {
		panic("arg list should start with paren. calling code should have asserted this")
	}

	ArgString := e[1:]

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
		// in the future we should probably add validation here that the key
		// can't contain otherwise-valid-name chars like {, }, etc
		if arg.etype == etName && e[0] == '=' {
			e = e[1:]
			argCont, eCont, errCont := Parse(e)
			if errCont != nil {
				return "", nil, nil, eCont, errCont
			}

			if eCont == "" {
				return "", nil, nil, "", ErrMissingComma
			}

			if argCont.etype != etInt && argCont.etype != etFloat && argCont.etype != etName && argCont.etype != etString && argCont.etype != etBool {
				return "", nil, nil, eCont, ErrBadArgumentStr{"int, float, name, bool or string", string(argCont.etype)}
			}

			if namedArgs == nil {
				namedArgs = make(map[string]*expr)
			}

			namedArgs[arg.str] = argCont

			e = eCont
		} else {
			posArgs = append(posArgs, arg)
		}

		// after the argument, trim any trailing spaces
		for len(e) > 0 && e[0] == ' ' {
			e = e[1:]
		}

		if e[0] == ')' {
			return ArgString[:len(ArgString)-len(e)], posArgs, namedArgs, e[1:], nil
		}

		if e[0] != ',' && e[0] != ' ' {
			return "", nil, nil, "", ErrUnexpectedCharacter
		}

		e = e[1:]
	}
}

var nameChar = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ!#$%&*+-/:;<>?@[]^_|~."

func isNameChar(r byte) bool {
	return strings.IndexByte(nameChar, r) >= 0
}

func isFnChar(r byte) bool {
	return false ||
		'a' <= r && r <= 'z' ||
		'A' <= r && r <= 'Z' ||
		'0' <= r && r <= '9'
}

func isDigit(r byte) bool {
	return '0' <= r && r <= '9'
}

func parseConst(s string) (*expr, string, error) {

	var i int
	var float bool
	// All valid characters for a floating-point constant
	// Just slurp them all in and let ParseFloat sort 'em out
	for i < len(s) && (isDigit(s[i]) || s[i] == '.' || s[i] == '+' || s[i] == '-' || s[i] == 'e' || s[i] == 'E') {
		// note that exponent syntax results into a float value.
		// so even values like 1e3 (1000) or 2000e-3 (2) which can be expressed as integers,
		// are considered floating point values.  if a function expets an int, then just don't use 'e' syntax.
		if s[i] == '.' || s[i] == 'e' || s[i] == 'E' {
			float = true
		}
		i++
	}

	if float {
		v, err := strconv.ParseFloat(s[:i], 64)
		return &expr{float: v, str: s[:i], etype: etFloat}, s[i:], err
	}
	v, err := strconv.ParseInt(s[:i], 10, 64)
	return &expr{int: v, str: s[:i], etype: etInt}, s[i:], err
}

// parseName parses a "name" which is either:
// 1) a metric expression
// 2) the key of a keyword argument
// 3) a function name
// It's up to the caller to determine which, based on context. E.g.:
// * if leftover data starts with '(' then assume 3 and validate function name
// * if leftover data starts with '=' assume 2
// * fallback to assuming 1.
// it returns the parsed name and any leftover data
func parseName(s string) (string, string) {

	var i int
	allowEqual := false

FOR:
	for braces := 0; i < len(s); i++ {
		// if the current expression is a metric name with ";" (59) we should
		// allow the "=" (61) character to be part of the metric name
		if isNameChar(s[i]) || (allowEqual && s[i] == 61) {
			if s[i] == 59 {
				allowEqual = true
			}
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

// caller must assure s starts with a single or double quote
func parseString(s string) (string, string, error) {

	if s[0] != '\'' && s[0] != '"' {
		panic("string should start with open quote. calling code should have asserted this")
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

	return s[:i], s[i+1:], nil
}

// extractMetric searches for a metric name in `m'
// metric name is defined to be a series of name characters terminated by a comma
func extractMetric(m string) string {
	start := 0
	end := 0
	curlyBraces := 0
	stringLiteral := false
	allowEqual := false
	for end < len(m) {
		if stringLiteral {
			if m[end] == '\'' || m[end] == '"' {
				stringLiteral = !stringLiteral
				start = end + 1
			}
		} else {
			if m[end] == '{' {
				curlyBraces++
			} else if m[end] == '}' {
				curlyBraces--
			} else if m[end] == ')' || (m[end] == ',' && curlyBraces == 0) {
				return m[start:end]
			} else if !(isNameChar(m[end]) || m[end] == ',' || (allowEqual && m[end] == '=')) {
				start = end + 1
				allowEqual = false
			} else if m[end] == ';' {
				allowEqual = true
			}
		}

		end++
	}

	return m[start:end]
}
