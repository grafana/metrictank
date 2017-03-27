package expr

//go:generate stringer -type=exprType
type exprType int

const (
	etName exprType = iota // e.g. a metric query pattern like foo.bar or foo.*.baz
	etFunc
	etConst
	etString
)

type expr struct {
	target    string // the name of a etName, or func name for etFunc. not used for etConst
	etype     exprType
	val       float64 // for etConst
	valStr    string  // for etString, and to store original input for etConst
	args      []*expr // positional
	namedArgs map[string]*expr
	argString string // for etFunc: literal string of how all the args were specified
}
