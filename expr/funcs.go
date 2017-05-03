package expr

import "github.com/raintank/metrictank/api/models"

//go:generate stringer -type=argType
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
	boolean                    // True or False
)

type optArg struct {
	key string
	val argType
}

// ExecHandler executes the function with its arguments.
// it is passed in:
// * a map of all input data it may need
// supported return values: []models.Series
type execHandler func(map[Req][]models.Series) ([]models.Series, error)

type GraphiteFunc interface {
	// Plan validates and processes both the mandatory arguments as well as the optional keyword arguments.
	Plan([]*expr, map[string]*expr, *Plan) (execHandler, error)
}

type funcConstructor func() GraphiteFunc

type funcDef struct {
	constr funcConstructor
	stable bool
}

var funcs map[string]funcDef

func init() {
	// keys must be sorted alphabetically. but functions with aliases can go together, in which case they are sorted by the first of their aliases
	funcs = map[string]funcDef{
		"alias":          {NewAlias, true},
		"avg":            {NewAvgSeries, true},
		"averageSeries":  {NewAvgSeries, true},
		"consolidateBy":  {NewConsolidateBy, true},
		"movingAverage":  {NewMovingAverage, false},
		"perSecond":      {NewPerSecond, true},
		"smartSummarize": {NewSmartSummarize, false},
		"sum":            {NewSumSeries, true},
		"sumSeries":      {NewSumSeries, true},
	}
}
