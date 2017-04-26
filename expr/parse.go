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
		return &expr{float: val, str: valStr, etype: etConst}, e, err
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
		exp := &expr{str: name, etype: etFunc}

		argString, posArgs, namedArgs, e, err := parseArgList(e)
		exp.argsStr = argString
		exp.args = posArgs
		exp.namedArgs = namedArgs

		return exp, e, err
	}

	return &expr{str: name, etype: etName}, e, nil
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

			namedArgs[arg.str] = &expr{
				etype: argCont.etype,
				float: argCont.float,
				str:   argCont.str,
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
