package expr

import (
	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/consolidation"
)

// Context describes a series timeframe and consolidator
type Context struct {
	from          uint32
	to            uint32
	consol        consolidation.Consolidator // can be 0 to mean undefined
	PNGroup       models.PNGroup             // pre-normalization group. if the data can be safely pre-normalized
	MDP           uint32                     // if we can MDP-optimize, reflects runtime consolidation MaxDataPoints. 0 otherwise
	optimizations Optimizations
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
	// * add the newly created slices into the dataMap so they can be reclaimed after the output is consumed
	// * not modify other properties on its input series, such as Tags map or Meta
	Exec(dataMap DataMap) ([]models.Series, error)
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
		"absolute":                     {NewAbsolute, true},
		"aggregate":                    {NewAggregate, true},
		"aggregateSeriesWithWildcards": {NewAggregateWithWildcardsConstructor(""), true},
		"alias":                        {NewAlias, true},
		"aliasByMetric":                {NewAliasByMetric, true},
		"aliasByTags":                  {NewAliasByNode, true},
		"aliasByNode":                  {NewAliasByNode, true},
		"aliasSub":                     {NewAliasSub, true},
		"asPercent":                    {NewAsPercent, true},
		"avg":                          {NewAggregateConstructor("average"), true},
		"averageAbove":                 {NewFilterSeriesConstructor("average", ">"), true},
		"averageBelow":                 {NewFilterSeriesConstructor("average", "<="), true},
		"averageSeries":                {NewAggregateConstructor("average"), true},
		"averageSeriesWithWildcards":   {NewAggregateWithWildcardsConstructor("average"), true},
		"consolidateBy":                {NewConsolidateBy, true},
		"constantLine":                 {NewConstantLine, false},
		"countSeries":                  {NewCountSeries, true},
		"cumulative":                   {NewConsolidateByConstructor("sum"), true},
		"currentAbove":                 {NewFilterSeriesConstructor("last", ">"), true},
		"currentBelow":                 {NewFilterSeriesConstructor("last", "<="), true},
		"derivative":                   {NewDerivative, true},
		"diffSeries":                   {NewAggregateConstructor("diff"), true},
		"divideSeries":                 {NewDivideSeries, true},
		"divideSeriesLists":            {NewDivideSeriesLists, true},
		"exclude":                      {NewExclude, true},
		"fallbackSeries":               {NewFallbackSeries, true},
		"filterSeries":                 {NewFilterSeries, true},
		"grep":                         {NewGrep, true},
		"group":                        {NewGroup, true},
		"groupByNode":                  {NewGroupByNodesConstructor(true), true},
		"groupByNodes":                 {NewGroupByNodesConstructor(false), true},
		"groupByTags":                  {NewGroupByTags, true},
		"highest":                      {NewHighestLowestConstructor("", true), true},
		"highestAverage":               {NewHighestLowestConstructor("average", true), true},
		"highestCurrent":               {NewHighestLowestConstructor("current", true), true},
		"highestMax":                   {NewHighestLowestConstructor("max", true), true},
		"integral":                     {NewIntegral, true},
		"invert":                       {NewInvert, true},
		"isNonNull":                    {NewIsNonNull, true},
		"keepLastValue":                {NewKeepLastValue, true},
		"linearRegression":             {NewLinearRegression, true},
		"lowest":                       {NewHighestLowestConstructor("", false), true},
		"lowestAverage":                {NewHighestLowestConstructor("average", false), true},
		"lowestCurrent":                {NewHighestLowestConstructor("current", false), true},
		"max":                          {NewAggregateConstructor("max"), true},
		"maximumAbove":                 {NewFilterSeriesConstructor("max", ">"), true},
		"maximumBelow":                 {NewFilterSeriesConstructor("max", "<="), true},
		"maxSeries":                    {NewAggregateConstructor("max"), true},
		"min":                          {NewAggregateConstructor("min"), true},
		"minimumAbove":                 {NewFilterSeriesConstructor("min", ">"), true},
		"minimumBelow":                 {NewFilterSeriesConstructor("min", "<="), true},
		"minMax":                       {NewMinMax, true},
		"minSeries":                    {NewAggregateConstructor("min"), true},
		"multiplySeries":               {NewAggregateConstructor("multiply"), true},
		"multiplySeriesWithWildcards":  {NewAggregateWithWildcardsConstructor("multiply"), true},
		"movingAverage":                {NewMovingWindowParticular("average"), true},
		"movingMax":                    {NewMovingWindowParticular("max"), true},
		"movingMedian":                 {NewMovingWindowParticular("median"), true},
		"movingMin":                    {NewMovingWindowParticular("min"), true},
		"movingSum":                    {NewMovingWindowParticular("sum"), true},
		"movingWindow":                 {NewMovingWindowGeneric, true},
		"nonNegativeDerivative":        {NewNonNegativeDerivative, true},
		"offset":                       {NewOffset, true},
		"offsetToZero":                 {NewOffsetToZero, true},
		"perSecond":                    {NewPerSecond, true},
		"rangeOfSeries":                {NewAggregateConstructor("rangeOf"), true},
		"removeAbovePercentile":        {NewRemoveAboveBelowPercentileConstructor(true), true},
		"removeAboveValue":             {NewRemoveAboveBelowValueConstructor(true), true},
		"removeBelowPercentile":        {NewRemoveAboveBelowPercentileConstructor(false), true},
		"removeBelowValue":             {NewRemoveAboveBelowValueConstructor(false), true},
		"removeEmptySeries":            {NewRemoveEmptySeries, true},
		"round":                        {NewRound, true},
		"scale":                        {NewScale, true},
		"scaleToSeconds":               {NewScaleToSeconds, true},
		"smartSummarize":               {NewSmartSummarize, false},
		"sortBy":                       {NewSortByConstructor("", false), true},
		"sortByMaxima":                 {NewSortByConstructor("max", true), true},
		"sortByName":                   {NewSortByName, true},
		"sortByTotal":                  {NewSortByConstructor("sum", true), true},
		"stddevSeries":                 {NewAggregateConstructor("stddev"), true},
		"substr":                       {NewSubstr, true},
		"sum":                          {NewAggregateConstructor("sum"), true},
		"sumSeries":                    {NewAggregateConstructor("sum"), true},
		"sumSeriesWithWildcards":       {NewAggregateWithWildcardsConstructor("sum"), true},
		"summarize":                    {NewSummarize, true},
		"timeShift":                    {NewTimeShift, true},
		"transformNull":                {NewTransformNull, true},
		"unique":                       {NewUnique, true},
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

func consumeFuncs(dataMap DataMap, fns []GraphiteFunc) ([]models.Series, []string, error) {
	var series []models.Series
	var queryPatts []string
	for i := range fns {
		in, err := fns[i].Exec(dataMap)
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
