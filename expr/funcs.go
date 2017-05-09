package expr

import "github.com/raintank/metrictank/api/models"

type GraphiteFunc interface {
	// Signature declares input and output arguments (return values)
	// input args can be optional in which case they can be specified positionally or via keys if you want to specify params that come after un-specified optional params
	// the val pointers of each input Arg should point to a location accessible to the function,
	// so that the planner can set up the inputs for your function based on user input.
	// NewPlan() will only create the plan if the expressions it parsed correspond to the signatures provided by the function
	Signature() ([]Arg, []Arg)
	// NeedRange allows a func to express that to be able to return data in the given from-to, it will need input data in the returned from-to window.
	// (e.g. movingAverage of 5min needs data as of from-5min)
	// this function will be called after validating and setting up all non-series and non-serieslist parameters.
	// this way a function can convey the needed range by leveraging any relevant integer, string, bool, etc parameters.
	// after this function is called, series and serieslist inputs will be set up.
	NeedRange(from, to uint32) (uint32, uint32)
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
