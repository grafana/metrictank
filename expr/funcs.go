package expr

import "github.com/raintank/metrictank/api/models"

type argType uint8

// argument types. to let functions describe their inputs and outputs
// potentially not as strict as reality (e.g. movingAverage windowsize is categorized as a str) that's why we have the extra validation step
const (
	series      argType = iota // a single series
	seriesList                 // a list of series
	seriesLists                // one or multiple seriesLists
	integer                    // number without decimals
	float                      // number potentially with decimals
	str                        // string
)

type Func interface {
	// Signature returns the list of argument types that must be provided as input, and those that will be returned
	// NewPlan() will only create the plan of the expressions it parsed correspond to the signatures provided by the function
	Signature() ([]argType, []argType)
	// Init passes in the expressions parsed (they are pre-validated to correspond to the given signature via Signature() in terms of number and type)
	// So that you can initialize internal state and perform deeper validation of arguments for functions that have specific requirements (e.g. a number argument that can only be even)
	Init([]*expr) error
	// NeedRange allows a func to express that to be able to return data in the given from to, it will need input data in the returned from-to window.
	// (e.g. movingAverage of 5min needs data as of from-5min)
	NeedRange(from, to uint32) (uint32, uint32)
	// Exec executes the function with its arguments.
	// it is passed in a map of all input data it may need as well as:
	// etConst (number) -> float64
	// etString -> str
	// etName/etFunc -> []models.Series or models.Series if the previous function returned a series
	// supported return values: models.Series, []models.Series
	Exec(map[Req][]models.Series, ...interface{}) ([]interface{}, error)
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
		"movingAverage": {NewMovingAverage, false},
		"consolidateBy": {NewConsolidateBy, true},
	}
}
