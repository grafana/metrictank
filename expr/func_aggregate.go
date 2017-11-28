package expr

import (
	"strings"

	"github.com/grafana/metrictank/api/models"
	"gopkg.in/raintank/schema.v1"
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
	return context
}

func (s *FuncAggregate) Exec(cache map[Req][]models.Series) ([]models.Series, error) {
	series, queryPatts, err := consumeFuncs(cache, s.in)
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
	s.agg.function(series, &out)

	cons, queryCons := summarizeCons(series)
	name := s.agg.name + "Series(" + strings.Join(queryPatts, ",") + ")"
	output := models.Series{
		Target:       name,
		QueryPatt:    name,
		Datapoints:   out,
		Interval:     series[0].Interval,
		Consolidator: cons,
		QueryCons:    queryCons,
	}
	cache[Req{}] = append(cache[Req{}], output)

	return []models.Series{output}, nil
}
