// argument types. to let functions describe their inputs and outputs
package expr

type arg interface {
	Key() string
	Optional() bool
	Store(e *expr)
}

// a single series (not generally used as input, but to describe output)
type argSeries struct {
	key   string
	opt   bool
	store *Func
}

func (a argSeries) Key() string    { return a.key }
func (a argSeries) Optional() bool { return a.opt }
func (a argSeries) Store(e *expr)  {} // because we need to store a Func, not an expr, can't use this method here

// a list of series
type argSeriesList struct {
	key   string
	opt   bool
	store *Func
}

func (a argSeriesList) Key() string    { return a.key }
func (a argSeriesList) Optional() bool { return a.opt }
func (a argSeriesList) Store(e *expr)  {}

// one or more lists of series
type argSeriesLists struct {
	key   string
	opt   bool
	store *[]Func
}

func (a argSeriesLists) Key() string    { return a.key }
func (a argSeriesLists) Optional() bool { return a.opt }
func (a argSeriesLists) Store(e *expr)  {}

// number without decimals
type argInt struct {
	key       string
	opt       bool
	validator []validator
	store     *int64
}

func (a argInt) Key() string    { return a.key }
func (a argInt) Optional() bool { return a.opt }
func (a argInt) Store(e *expr)  { *a.store = e.int }

// one or more numbers without decimals
type argInts struct {
	key       string
	opt       bool
	validator []validator
	store     *[]int64
}

func (a argInts) Key() string    { return a.key }
func (a argInts) Optional() bool { return a.opt }
func (a argInts) Store(e *expr)  { *a.store = append(*a.store, e.int) }

// floating point number; potentially with decimals
type argFloat struct {
	key       string
	opt       bool
	validator []validator
	store     *float64
}

func (a argFloat) Key() string    { return a.key }
func (a argFloat) Optional() bool { return a.opt }
func (a argFloat) Store(e *expr)  { *a.store = e.float }

// string
type argString struct {
	key       string
	opt       bool
	validator []validator
	store     *string
}

func (a argString) Key() string    { return a.key }
func (a argString) Optional() bool { return a.opt }
func (a argString) Store(e *expr)  { *a.store = e.str }

// True or False
type argBool struct {
	key   string
	opt   bool
	store *bool
}

func (a argBool) Key() string    { return a.key }
func (a argBool) Optional() bool { return a.opt }
func (a argBool) Store(e *expr)  { *a.store = e.bool }
