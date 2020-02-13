package expr

import (
	"strings"
	"unsafe"

	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/schema"
)

type FuncAggregate struct {
	in  []GraphiteFunc
	agg seriesAggregator
}

// NewAggregateConstructor takes an agg string and returns a constructor function
func NewAggregateConstructor(aggDescription string, aggFunc crossSeriesAggFunc) func() GraphiteFunc {
	return func() GraphiteFunc {
		return &FuncAggregate{agg: seriesAggregator{function: aggFunc, name: aggDescription}}
	}
}

func (s *FuncAggregate) Signature() ([]Arg, []Arg) {
	return []Arg{
		ArgSeriesLists{val: &s.in},
	}, []Arg{ArgSeries{}}
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

	if len(series) == 1 {
		name := s.agg.name + "Series(" + series[0].QueryPatt + ")"
		series[0].Target = name
		series[0].QueryPatt = name
		return series, nil
	}
	out := pointSlicePool.Get().([]schema.Point)
	series = Normalize(dataMap, series)
	s.agg.function(series, &out)

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
	name := s.agg.name + "Series(" + strings.Join(queryPatts, ",") + ")"
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
