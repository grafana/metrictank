package expr

import (
	"fmt"
	"net/http"
	"reflect"
	"strconv"
	"strings"

	"github.com/grafana/metrictank/internal/util"
	"github.com/grafana/metrictank/pkg/api/models"
	"github.com/grafana/metrictank/pkg/errors"
	log "github.com/sirupsen/logrus"
)

var (
	ErrMissingArg          = errors.NewBadRequest("argument missing")
	ErrTooManyArg          = errors.NewBadRequest("too many arguments")
	ErrMissingTimeseries   = errors.NewBadRequest("missing time series argument")
	ErrWildcardNotAllowed  = errors.NewBadRequest("found wildcard where series expected")
	ErrMissingExpr         = errors.NewBadRequest("missing expression")
	ErrMissingComma        = errors.NewBadRequest("missing comma")
	ErrMissingQuote        = errors.NewBadRequest("missing quote")
	ErrUnexpectedCharacter = errors.NewBadRequest("unexpected character")
	ErrIllegalCharacter    = errors.NewBadRequest("illegal character for function name")
	ErrExpectingPipeFunc   = errors.NewBadRequest("pipe symbol must be followed by function call")
	ErrIncompleteCall      = errors.NewBadRequest("incomplete function call")
)

type ErrBadArgument struct {
	exp reflect.Type
	got reflect.Type
}

func (e ErrBadArgument) Error() string {
	return fmt.Sprintf("argument bad type. expected %s - got %s", e.exp, e.got)
}

func (e ErrBadArgument) HTTPStatusCode() int {
	return http.StatusBadRequest
}

type ErrBadArgumentStr struct {
	exp string
	got string
}

func (e ErrBadArgumentStr) Error() string {
	return fmt.Sprintf("argument bad type. expected %s - got %s", e.exp, e.got)
}

func (e ErrBadArgumentStr) HTTPStatusCode() int {
	return http.StatusBadRequest
}

type ErrBadRegex struct {
	error
}

func (e ErrBadRegex) HTTPStatusCode() int {
	return http.StatusBadRequest
}

type ErrUnknownFunction string

func (e ErrUnknownFunction) Error() string {
	return fmt.Sprintf("unknown function %q", string(e))
}

func (e ErrUnknownFunction) HTTPStatusCode() int {
	return http.StatusBadRequest
}

type ErrUnknownKwarg struct {
	key string
}

func (e ErrUnknownKwarg) Error() string {
	return fmt.Sprintf("unknown keyword argument %q", e.key)
}

func (e ErrUnknownKwarg) HTTPStatusCode() int {
	return http.StatusBadRequest
}

type ErrBadKwarg struct {
	key string
	exp Arg
	got exprType
}

func (e ErrBadKwarg) Error() string {
	return fmt.Sprintf("keyword argument %q bad type. expected %T - got %s", e.key, e.exp, e.got)
}

func (e ErrBadKwarg) HTTPStatusCode() int {
	return http.StatusBadRequest
}

type ErrKwargSpecifiedTwice struct {
	key string
}

func (e ErrKwargSpecifiedTwice) Error() string {
	return fmt.Sprintf("keyword argument %q specified twice", e.key)
}

func (e ErrKwargSpecifiedTwice) HTTPStatusCode() int {
	return http.StatusBadRequest
}

type MetricRequest struct {
	Metric string
	From   int32
	Until  int32
}

// ParseContext includes contextual information that affects how the current
// expression is parsed.
// This should only be set by internal calls.
type ParseContext struct {
	// Piped means the expression to be parsed is known to receive a piped
	// input.
	Piped bool
	// CanParseAsNumber means the expression returned from Parse() can be a
	// number. Only a full, non-piped argument for a function call can be
	// parsed as a number.
	// For example, given the string "funcA(1 | funcB(2), 3)":
	// "1" should not be parsed as a number. While it is in the first funcA
	// argument, it is not the entire argument (there are additional
	// characters "| funcB(2)" before the comma indicating the end of one
	// argument). While "1" could also be seen as an argument to funcB, a
	// number cannot be the input to a pipe. It should instead be parsed as a
	// metric named "1".
	// "2" is a full argument for funcB, so it should be parsed as a number.
	// "3" is a full argument for funcA, so it should be parsed as a number.
	CanParseAsNumber bool
}

// ParseMany parses a slice of strings into a slice of expressions (recursively)
// not included: validation that requested functions exist, correct args are passed, etc.
func ParseMany(targets []string) ([]*expr, error) {
	var out []*expr
	for _, target := range targets {
		e, leftover, err := Parse(target, ParseContext{false, false})
		if err != nil {
			return nil, err
		}
		if leftover != "" {
			return nil, errors.NewBadRequestf("failed to parse %q fully. got leftover %q", target, leftover)
		}
		out = append(out, e)
	}
	return out, nil
}

func skipWhitespace(s string) string {
	for len(s) >= 1 && s[0] == ' ' {
		s = s[1:]
	}
	return s
}

// Parses an expression string and turns it into an expression
// also returns any leftover data that could not be parsed
func Parse(e string, pCtx ParseContext) (*expr, string, error) {
	e = skipWhitespace(e)

	if len(e) == 0 {
		return nil, "", ErrMissingExpr
	}

	if pCtx.CanParseAsNumber && isNumStartChar(e[0]) {
		constExpr, leftover, err := parseNumber(e)
		if err != nil {
			return nil, "", err
		}
		leftover = skipWhitespace(leftover)
		if leftover == "" || leftover[0] == ',' || leftover[0] == ')' {
			return constExpr, leftover, nil
		}
	}

	if val, ok := strToBool(e); ok {
		// 'false' is 5 chars, 'true' is 4
		size := 5
		if val {
			size = 4
		}
		return &expr{bool: val, str: e[:size], etype: etBool}, e[size:], nil
	}

	if e[0] == '\'' || e[0] == '"' {
		val, e, err := parseString(e)
		return &expr{str: val, etype: etString}, e, err
	}

	name, e := parseName(e)

	if name == "" {
		return nil, e, ErrMissingArg
	}

	var exp *expr
	var err error

	if e != "" && e[0] == '(' && isFnStartChar(name[0]) {
		// in this case, our parsed name is a function
		for i := range name {
			if !isFnChar(name[i]) {
				return nil, "", ErrIllegalCharacter
			}
		}

		exp = &expr{str: name, etype: etFunc}

		exp.argsStr, exp.args, exp.namedArgs, e, err = parseArgList(e, pCtx.Piped)

		if err != nil {
			return exp, e, err
		}
	} else {
		// otherwise it's a metric name/pattern
		exp = &expr{str: name, etype: etName}
	}

	// both a metricname/pattern or a function may be followed by one or more pipe symbol + other function call
	// let's say the input is "A | B | C | D"
	// we process like so:
	// 1) we parsed A already when we're here
	// 2) see a pipe, parse B, and put A as the first arg into B (let's call this B+ here)
	// 3) see another pipe, parse C, and put B+ as first arg into it, which gives C+
	// 4) see another pipe and parse D, and we put C+ into it as first arg.
	// note that every time we read beyond a pipe, we call this same Parse function again
	// for this to work correctly, we should only do this discovery/parsing of the pipe chain in the top level call
	// if we are in a sub-call to Parse (such as the Parse() called in step 2/3/4) we should not try to read upcoming
	// pipes, but rather have the top-level consume them all.

	for !pCtx.Piped && e != "" {
		e = skipWhitespace(e)
		if e == "" || e[0] != '|' {
			break
		}
		e = e[1:]
		var nextExp *expr
		nextExp, e, err = Parse(e, ParseContext{Piped: true, CanParseAsNumber: false})
		if err != nil {
			return exp, e, err
		}
		if nextExp.etype != etFunc {
			return exp, e, ErrExpectingPipeFunc
		}
		realArgs := nextExp.args
		nextExp.args = []*expr{
			exp,
		}
		nextExp.args = append(nextExp.args, realArgs...)
		exp = nextExp
	}

	return exp, e, nil
}

func strToBool(val string) (bool, bool) {
	if strings.HasPrefix(val, "True") || strings.HasPrefix(val, "true") {
		return true, true
	}

	if strings.HasPrefix(val, "False") || strings.HasPrefix(val, "false") {
		return false, true
	}

	return false, false
}

func strToInt(val string) (int, bool) {
	i, err := strconv.Atoi(val)
	if err != nil {
		return 0, false
	}
	return i, true
}

func strToFloat(val string) (float64, bool) {
	f, err := strconv.ParseFloat(val, 64)
	if err != nil {
		return 0, false
	}
	return f, true
}

// parseArgList parses a comma separated list of arguments
// (recursively, so any args that are themselves function calls get parsed also)
// caller must assure s starts with opening paren "("
// parsing ends when the respective closing ")" is encountered (or an error).
// Note: not the first closing ")". e.g. f1(f2(a),b) will parse as:
// f1 func with args:
//   - f2 func with args:
//   - a
//   - b
func parseArgList(e string, piped bool) (string, []*expr, map[string]*expr, string, error) {

	var (
		posArgs   []*expr
		namedArgs map[string]*expr
	)

	if e[0] != '(' {
		panic("arg list should start with paren. calling code should have asserted this")
	}
	ArgString := e[1:]
	e = e[1:] // leftover

	// arglist may be empty only if the function was piped into, but still requires a closing parenthesis
	e = skipWhitespace(e)
	if piped {
		if e == "" {
			return "", nil, nil, "", ErrIncompleteCall
		}
		if e[0] == ')' {
			return "", nil, nil, e[1:], nil
		}
	}

	for {
		if e == "" {
			if len(posArgs) > 0 || len(namedArgs) > 0 {
				// after any arg we should always have a ')' or ','
				return "", nil, nil, "", ErrIncompleteCall
			}
			// if it's our first time here then we are missing an argument
			return "", nil, nil, "", ErrMissingArg
		}

		var arg *expr
		var err error
		arg, e, err = Parse(e, ParseContext{Piped: false, CanParseAsNumber: true})
		if err != nil {
			return "", nil, nil, e, err
		}

		if e == "" {
			return "", nil, nil, "", ErrIncompleteCall
		}

		// we now know we're parsing a key-value pair
		// in the future we should probably add validation here that the key
		// can't contain otherwise-valid-name chars like {, }, etc
		if arg.etype == etName && e[0] == '=' {
			e = e[1:]
			argCont, eCont, errCont := Parse(e, ParseContext{Piped: false, CanParseAsNumber: true})
			if errCont != nil {
				return "", nil, nil, eCont, errCont
			}

			if eCont == "" {
				return "", nil, nil, "", ErrMissingComma
			}

			if argCont.etype != etInt && argCont.etype != etFloat && argCont.etype != etName && argCont.etype != etString && argCont.etype != etBool {
				return "", nil, nil, eCont, ErrBadArgumentStr{"int, float, name, bool or string", argCont.etype.String()}
			}

			if namedArgs == nil {
				namedArgs = make(map[string]*expr)
			}

			namedArgs[arg.str] = argCont

			e = eCont
		} else {
			posArgs = append(posArgs, arg)
		}

		e = skipWhitespace(e)

		if e[0] == ')' {
			return ArgString[:len(ArgString)-len(e)], posArgs, namedArgs, e[1:], nil
		}

		if e[0] != ',' && e[0] != ' ' {
			return "", nil, nil, "", ErrUnexpectedCharacter
		}

		e = e[1:]
	}
}

var nameChar = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ!#$%&*+-/:;<>?@[]^_~."

func isNameChar(r byte) bool {
	return strings.IndexByte(nameChar, r) >= 0
}

func isFnStartChar(r byte) bool {
	return 'a' <= r && r <= 'z' ||
		'A' <= r && r <= 'Z'
}

func isFnChar(r byte) bool {
	return false ||
		'a' <= r && r <= 'z' ||
		'A' <= r && r <= 'Z' ||
		'0' <= r && r <= '9'
}

func isNumStartChar(r byte) bool {
	return '0' <= r && r <= '9' || r == '-' || r == '+'
}

func parseNumber(s string) (*expr, string, error) {

	var i int
	var float bool
	// All valid characters for a floating-point constant
	// Just slurp them all in and let ParseFloat sort 'em out
	for i < len(s) && (util.IsDigit(s[i]) || s[i] == '.' || s[i] == '+' || s[i] == '-' || s[i] == 'e' || s[i] == 'E') {
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

// extractMetric searches for a metric name or path Expression in `m'
// metric name / path expression is defined by the following criteria:
//  1. Not a function name
//  2. Consists only of name characters
//     2.1 '=' and ' ' are conditionally allowed if ';' is found (denoting tag format)
//     2.2 ',' is conditionally allowed within matching '{}'
//  3. Is not a string literal (i.e. contained within single/double quote pairs)
func extractMetric(m string) string {
	start := 0
	end := 0
	curlyBraces := 0
	quoteChar := byte(0)
	allowTagChars := false
	for end < len(m) {
		c := m[end]
		if (c == '\'' || c == '"') && (end == 0 || m[end-1] != '\\') {
			// Found a non-escaped quote char
			if quoteChar == 0 {
				quoteChar = c
				start = end + 1
			} else if c == quoteChar {
				quoteChar = byte(0)
				start = end + 1
			}
		} else if quoteChar == 0 {
			if c == '{' {
				curlyBraces++
			} else if c == '}' {
				curlyBraces--
			} else if c == ')' || (c == ',' && curlyBraces == 0) {
				return m[start:end]
			} else if !(isNameChar(c) || c == ',' || (allowTagChars && strings.IndexByte("= ", c) >= 0)) {
				start = end + 1
				allowTagChars = false
			} else if c == ';' {
				allowTagChars = true
			}
		}

		end++
	}

	if quoteChar != 0 {
		log.Warnf("extractMetric: encountered unterminated string literal in %s", m)
		return ""
	}

	return m[start:end]
}

// aggKey returns a string key by applying the selectors
// (integers for node positions or strings for tag names) to the given serie
func aggKey(serie models.Series, nodes []expr) string {
	metric := extractMetric(serie.Target)
	if len(metric) == 0 {
		metric = serie.Tags["name"]
	}
	// Trim off tags (if they are there) and split on '.'
	parts := strings.Split(strings.SplitN(metric, ";", 2)[0], ".")
	var name []string
	for _, n := range nodes {
		if n.etype == etInt {
			idx := int(n.int)
			if idx < 0 {
				idx += len(parts)
			}
			if idx >= len(parts) || idx < 0 {
				continue
			}
			name = append(name, parts[idx])
		} else if n.etype == etString {
			s := n.str
			name = append(name, serie.Tags[s])
		}
	}
	return strings.Join(name, ".")
}

// filterNodesByPositions removes the node at the given position(s)
// it is assumed that the node expressions are int ettype.
func filterNodesByPositions(name string, nodes []expr) string {
	parts := strings.Split(name, ".")
	var newName []string

	for i, word := range parts {
		var found bool
		for _, n := range nodes {
			found = (n.int == int64(i)) || found
		}
		if !found {
			newName = append(newName, word)
		}
	}
	return strings.Join(newName, ".")
}
