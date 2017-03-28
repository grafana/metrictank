package expr

import "github.com/raintank/metrictank/api/models"

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
	Init([]*expr) error                                                  // initialize and validate arguments (expressions given by user), for functions that have specific requirements
	Depends(from, to uint32) (uint32, uint32)                            // allows a func to express its dependencies
	Exec(map[Req][]models.Series, ...interface{}) ([]interface{}, error) // execute the function with its arguments. functions and names resolved to series, other expression types(constant, strings, etc) are still the bare expressions that were also passed into Init()
}

type funcConstructor func() Func

type funcDef struct {
	constr funcConstructor
	stable bool
}

var funcs map[string]funcDef

func init() {
	funcs = map[string]funcDef{
		"alias":         {NewAlias, true},
		"sum":           {NewSumSeries, true},
		"sumSeries":     {NewSumSeries, true},
		"avg":           {NewAvgSeries, true},
		"averageSeries": {NewAvgSeries, true},
		"movingAverage": {NewMovingAverage, true},
		"consolidateBy": {NewConsolidateBy, false},
	}
}
