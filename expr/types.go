// argument types. to let functions describe their inputs and outputs
package expr

type arg interface {
	Key() string
	Optional() bool
}

// a single series (not generally used as input, but to describe output)
type argSeries struct {
	key string
	opt bool
	val *Func
}

func (a argSeries) Key() string    { return a.key }
func (a argSeries) Optional() bool { return a.opt }

// a list of series
type argSeriesList struct {
	key string
	opt bool
	val *Func
}

func (a argSeriesList) Key() string    { return a.key }
func (a argSeriesList) Optional() bool { return a.opt }

// one or more lists of series
type argSeriesLists struct {
	key string
	opt bool
	val *[]Func
}

func (a argSeriesLists) Key() string    { return a.key }
func (a argSeriesLists) Optional() bool { return a.opt }

// number without decimals
type argInt struct {
	key       string
	opt       bool
	validator []validator
	val       *int64
}

func (a argInt) Key() string    { return a.key }
func (a argInt) Optional() bool { return a.opt }

// one or more numbers without decimals
type argInts struct {
	key       string
	opt       bool
	validator []validator
	val       *[]int64
}

func (a argInts) Key() string    { return a.key }
func (a argInts) Optional() bool { return a.opt }

// floating point number; potentially with decimals
type argFloat struct {
	key       string
	opt       bool
	validator []validator
	val       *float64
}

func (a argFloat) Key() string    { return a.key }
func (a argFloat) Optional() bool { return a.opt }

// string
type argString struct {
	key       string
	opt       bool
	validator []validator
	val       *string
}

func (a argString) Key() string    { return a.key }
func (a argString) Optional() bool { return a.opt }

// True or False
type argBool struct {
	key string
	opt bool
	val *bool
}

func (a argBool) Key() string    { return a.key }
func (a argBool) Optional() bool { return a.opt }
