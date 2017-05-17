package expr

import "github.com/raintank/metrictank/api/models"

type Context struct {
	from   uint32
	to     uint32
}

type GraphiteFunc interface {
	// Signature declares input and output arguments (return values)
	// input args can be optional in which case they can be specified positionally or via keys if you want to specify params that come after un-specified optional params
	// the val pointers of each input Arg should point to a location accessible to the function,
	// so that the planner can set up the inputs for your function based on user input.
	// NewPlan() will only create the plan if the expressions it parsed correspond to the signatures provided by the function
	Signature() ([]Arg, []Arg)

	// Context allows a func to alter the context that will be passed down the expression tree.
	// this function will be called after validating and setting up all non-series and non-serieslist parameters.
	// (as typically, context alterations require integer/string/bool/etc parameters, and shall affect series[list] parameters)
	// examples:
	// * movingAverage(foo,5min) -> the 5min arg will be parsed, so we can request 5min of earlier data, which will affect the request for foo.
	Context(c Context) Context
	// Exec executes the function. the function should call any input functions, do its processing, and return output.
	// IMPORTANT: for performance and correctness, functions should
	// * not modify slices of points that they get from their inputs
	// * use the pool to get new slices in which to store any new/modified dat
	// * add the newly created slices into the cache so they can be reclaimed after the output is consumed
	Exec(map[Req][]models.Series) ([]models.Series, error)
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
		"aliasByNode":    {NewAliasByNode, true},
		"aliasSub":       {NewAliasSub, true},
		"avg":            {NewAvgSeries, true},
		"averageSeries":  {NewAvgSeries, true},
		"consolidateBy":  {NewConsolidateBy, true},
		"divideSeries":   {NewDivideSeries, true},
		"movingAverage":  {NewMovingAverage, false},
		"perSecond":      {NewPerSecond, true},
		"scale":          {NewScale, true},
		"smartSummarize": {NewSmartSummarize, false},
		"sum":            {NewSumSeries, true},
		"sumSeries":      {NewSumSeries, true},
		"transformNull":  {NewTransformNull, true},
	}
}
