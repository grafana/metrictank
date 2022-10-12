package expr

import (
	"math"
	"strings"
	"unsafe"

	"github.com/grafana/metrictank/pkg/api/models"
)

type FuncAggregate struct {
	in           []GraphiteFunc
	name         string
	xFilesFactor float64
}

// NewAggregateConstructor takes an agg string and returns a constructor function
func NewAggregateConstructor(name string) func() GraphiteFunc {
	return func() GraphiteFunc {
		return &FuncAggregate{name: name}
	}
}

func NewAggregate() GraphiteFunc {
	return &FuncAggregate{}
}

func (s *FuncAggregate) Signature() ([]Arg, []Arg) {
	if s.name == "" {
		return []Arg{
			ArgSeriesLists{val: &s.in},
			ArgString{val: &s.name, validator: []Validator{IsAggFunc}, key: "func"},
			ArgFloat{val: &s.xFilesFactor, opt: true, key: "xFilesFactor"},
		}, []Arg{ArgSeries{}}
	} else {
		return []Arg{
			ArgSeriesLists{val: &s.in},
		}, []Arg{ArgSeries{}}
	}

}

func (s *FuncAggregate) Context(context Context) Context {
	context.PNGroup = models.PNGroup(uintptr(unsafe.Pointer(s)))
	return context
}

func (s *FuncAggregate) Exec(dataMap DataMap) ([]models.Series, error) {
	series, queryPatts, err := consumeFuncs(dataMap, s.in)
	if err != nil {
		return nil, err
	}

	if len(series) == 0 {
		return series, nil
	}

	agg := seriesAggregator{function: getCrossSeriesAggFunc(s.name), name: s.name}
	series = Normalize(series, NewCOWCycler(dataMap))
	return aggregate(dataMap, series, queryPatts, agg, s.xFilesFactor)
}

// aggregate aggregates series using the requested aggregator and xFilesFactor and returns an output slice of length 1.
func aggregate(dataMap DataMap, series []models.Series, queryPatts []string, agg seriesAggregator, xFilesFactor float64) ([]models.Series, error) {
	if len(series) == 1 {
		name := agg.name + "Series(" + series[0].QueryPatt + ")"
		series[0].Target = name
		series[0].QueryPatt = name
		return series, nil
	}
	out := pointSlicePool.GetMin(len(series[0].Datapoints))

	agg.function(series, &out)

	//remove values in accordance to xFilesFactor
	if !skipCrossSeriesXff(xFilesFactor) {
		for i := 0; i < len(series[0].Datapoints); i++ {
			if !crossSeriesXff(series, i, xFilesFactor) {
				out[i].Val = math.NaN()
			}
		}
	}

	// The tags for the aggregated series is only the tags that are
	// common to all input series
	commonTags := series[0].CopyTags()

	var meta models.SeriesMeta

	for _, serie := range series {
		meta = meta.Merge(serie.Meta)
		for k, v := range serie.Tags {
			if commonTags[k] != v {
				delete(commonTags, k)
			}
		}
	}

	cons, queryCons := summarizeCons(series)
	name := agg.name + "Series(" + strings.Join(queryPatts, ",") + ")"

	commonTags["aggregatedBy"] = agg.name
	if _, ok := commonTags["name"]; !ok {
		commonTags["name"] = name
	}

	output := series[0]
	output.Target = name
	output.QueryPatt = name
	output.Tags = commonTags
	output.Datapoints = out
	output.QueryCons = queryCons
	output.Consolidator = cons
	output.Meta = meta

	dataMap.Add(Req{}, output)

	return []models.Series{output}, nil
}
