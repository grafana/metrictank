package expr

type argType uint8

// argument type. potentially not as strict as reality (e.g. movingAverage windowsize is categorized as a str) that's why we have the extra validation step
const (
	series argType = iota
	seriesList
	integer // number without decimals
	float   // number potentially with decimals
	str     // string
)

type Func interface {
	Signature() ([]argType, []argType)
	// what can be assumed to have been pre-validated: len of args, and basic types (e.g. seriesList)
	Init([]*expr) error                       // initialize and validate arguments, for functions that have specific requirements
	Depends(from, to uint32) (uint32, uint32) // allows a func to express its dependencies
}

type funcConstructor func() Func

type funcDef struct {
	constr funcConstructor
	stable bool
}

var funcs map[string]funcDef

func init() {
	funcs = map[string]funcDef{
		"sum":           funcDef{NewSumSeries, true},
		"sumSeries":     funcDef{NewSumSeries, true},
		"movingAverage": funcDef{NewMovingAverage, true},
		"consolidateBy": funcDef{NewConsolidateBy, false},
	}
}
