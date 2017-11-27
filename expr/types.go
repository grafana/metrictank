// argument types. to let functions describe their inputs and outputs
package expr

import "regexp"

// Arg is an argument to a GraphiteFunc
// note how every implementation has a val property.
// this property should point to value accessible to the function.
// the value will be set up by the planner; it assures that
// by the time Func.Exec() is called, the function has access to all
// needed inputs, whether simple values, or in the case of ArgSeries*
// inputs other functions to call which will feed it data.
type Arg interface {
	Key() string
	Optional() bool
}

// ArgSeries is a single series argument
// not generally used as input since graphite functions typically take multiple series as input
// but is useful to describe output
type ArgSeries struct {
	key string
	opt bool
	val *GraphiteFunc
}

func (a ArgSeries) Key() string    { return a.key }
func (a ArgSeries) Optional() bool { return a.opt }

// ArgSeriesList is a list of series argument, it can be 0..N series
type ArgSeriesList struct {
	key string
	opt bool
	val *GraphiteFunc
}

func (a ArgSeriesList) Key() string    { return a.key }
func (a ArgSeriesList) Optional() bool { return a.opt }

// ArgSeriesLists represents one or more lists of series inputs.
type ArgSeriesLists struct {
	key string
	opt bool
	val *[]GraphiteFunc
}

func (a ArgSeriesLists) Key() string    { return a.key }
func (a ArgSeriesLists) Optional() bool { return a.opt }

// ArgInt is a number without decimals
type ArgInt struct {
	key       string
	opt       bool
	validator []Validator
	val       *int64
}

func (a ArgInt) Key() string    { return a.key }
func (a ArgInt) Optional() bool { return a.opt }

// ArgInts represents one or more numbers without decimals
type ArgInts struct {
	key       string
	opt       bool
	validator []Validator
	val       *[]int64
}

func (a ArgInts) Key() string    { return a.key }
func (a ArgInts) Optional() bool { return a.opt }

// floating point number; potentially with decimals
type ArgFloat struct {
	key       string
	opt       bool
	validator []Validator
	val       *float64
}

func (a ArgFloat) Key() string    { return a.key }
func (a ArgFloat) Optional() bool { return a.opt }

// string
type ArgString struct {
	key       string
	opt       bool
	validator []Validator
	val       *string
}

func (a ArgString) Key() string    { return a.key }
func (a ArgString) Optional() bool { return a.opt }

// ArgStrings represents one or more strings
type ArgStrings struct {
	key       string
	opt       bool
	validator []Validator
	val       *[]string
}

func (a ArgStrings) Key() string    { return a.key }
func (a ArgStrings) Optional() bool { return a.opt }

// like string, but should result in a regex
type ArgRegex struct {
	key       string
	opt       bool
	validator []Validator
	val       **regexp.Regexp
}

func (a ArgRegex) Key() string    { return a.key }
func (a ArgRegex) Optional() bool { return a.opt }

// True or False
type ArgBool struct {
	key string
	opt bool
	val *bool
}

func (a ArgBool) Key() string    { return a.key }
func (a ArgBool) Optional() bool { return a.opt }

// Array of mixed strings or ints
type ArgStringsOrInts struct {
	key       string
	opt       bool
	validator []Validator
	val       *[]expr
}

func (a ArgStringsOrInts) Key() string    { return a.key }
func (a ArgStringsOrInts) Optional() bool { return a.opt }
