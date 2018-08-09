package expr

import (
	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/consolidation"
)

// Context describes a series timeframe and consolidator
type Context struct {
	from   uint32
	to     uint32
	consol consolidation.Consolidator // can be 0 to mean undefined
}

// GraphiteFunc defines a graphite processing function
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
	// * consolidateBy(bar, "sum") -> the "sum" arg will be parsed, so we can pass on the fact that bar needs to be sum-consolidated
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
		"alias":             {NewAlias, true},
		"aliasByTags":       {NewAliasByNode, true},
		"aliasByNode":       {NewAliasByNode, true},
		"aliasSub":          {NewAliasSub, true},
		"avg":               {NewAggregateConstructor("average", crossSeriesAvg), true},
		"averageSeries":     {NewAggregateConstructor("average", crossSeriesAvg), true},
		"consolidateBy":     {NewConsolidateBy, true},
		"diffSeries":        {NewAggregateConstructor("diff", crossSeriesDiff), true},
		"divideSeries":      {NewDivideSeries, true},
		"divideSeriesLists": {NewDivideSeriesLists, true},
		"exclude":           {NewExclude, true},
		"filterSeries":      {NewFilterSeries, true},
		"grep":              {NewGrep, true},
		"groupByTags":       {NewGroupByTags, true},
		"isNonNull":         {NewIsNonNull, true},
		"max":               {NewAggregateConstructor("max", crossSeriesMax), true},
		"maxSeries":         {NewAggregateConstructor("max", crossSeriesMax), true},
		"min":               {NewAggregateConstructor("min", crossSeriesMin), true},
		"minSeries":         {NewAggregateConstructor("min", crossSeriesMin), true},
		"multiplySeries":    {NewAggregateConstructor("multiply", crossSeriesMultiply), true},
		"movingAverage":     {NewMovingAverage, false},
		"perSecond":         {NewPerSecond, true},
		"rangeOfSeries":     {NewAggregateConstructor("rangeOf", crossSeriesRange), true},
		"scale":             {NewScale, true},
		"scaleToSeconds":    {NewScaleToSeconds, true},
		"smartSummarize":    {NewSmartSummarize, false},
		"sortByName":        {NewSortByName, true},
		"stddevSeries":      {NewAggregateConstructor("stddev", crossSeriesStddev), true},
		"sum":               {NewAggregateConstructor("sum", crossSeriesSum), true},
		"sumSeries":         {NewAggregateConstructor("sum", crossSeriesSum), true},
		"summarize":         {NewSummarize, true},
		"transformNull":     {NewTransformNull, true},
	}
}

// summarizeCons returns the first explicitly specified Consolidator, QueryCons for the given set of input series,
// or the first one, otherwise.
func summarizeCons(series []models.Series) (consolidation.Consolidator, consolidation.Consolidator) {
	for _, serie := range series {
		if serie.QueryCons != 0 {
			return serie.Consolidator, serie.QueryCons
		}
	}
	return series[0].Consolidator, series[0].QueryCons
}

func consumeFuncs(cache map[Req][]models.Series, fns []GraphiteFunc) ([]models.Series, []string, error) {
	var series []models.Series
	var queryPatts []string
	for i := range fns {
		in, err := fns[i].Exec(cache)
		if err != nil {
			return nil, nil, err
		}
		if len(in) != 0 {
			series = append(series, in...)
			queryPatts = append(queryPatts, in[0].QueryPatt)
		}
	}
	return series, queryPatts, nil
}
