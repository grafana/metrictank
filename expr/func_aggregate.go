package expr

import (
	"strings"

	"github.com/grafana/metrictank/api/models"
	"github.com/raintank/schema"
)

type FuncAggregate struct {
	in   []GraphiteFunc
	name string
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
			ArgString{val: &s.name, validator: []Validator{IsAggFunc}},
		}, []Arg{ArgSeries{}}
	} else {
		return []Arg{
			ArgSeriesLists{val: &s.in},
		}, []Arg{ArgSeries{}}
	}

}

func (s *FuncAggregate) Context(context Context) Context {
	return context
}

func (s *FuncAggregate) Exec(cache map[Req][]models.Series) ([]models.Series, error) {
	series, queryPatts, err := consumeFuncs(cache, s.in)
	if err != nil {
		return nil, err
	}

	agg := seriesAggregator{function: getCrossSeriesAggFunc(s.name), name: s.name}

	if len(series) == 0 {
		return series, nil
	}

	if len(series) == 1 {
		name := agg.name + "Series(" + series[0].QueryPatt + ")"
		series[0].Target = name
		series[0].QueryPatt = name
		return series, nil
	}
	out := pointSlicePool.Get().([]schema.Point)
	agg.function(series, &out)

	// The tags for the aggregated series is only the tags that are
	// common to all input series
	commonTags := make(map[string]string, len(series[0].Tags))
	for k, v := range series[0].Tags {
		commonTags[k] = v
	}

	for _, serie := range series {
		for k, v := range serie.Tags {
			if commonTags[k] != v {
				delete(commonTags, k)
			}
		}
	}

	cons, queryCons := summarizeCons(series)
	name := agg.name + "Series(" + strings.Join(queryPatts, ",") + ")"
	commonTags["aggregatedBy"] = agg.name
	commonTags["name"] = name
	output := models.Series{
		Target:       name,
		QueryPatt:    name,
		Tags:         commonTags,
		Datapoints:   out,
		Interval:     series[0].Interval,
		Consolidator: cons,
		QueryCons:    queryCons,
	}
	cache[Req{}] = append(cache[Req{}], output)

	return []models.Series{output}, nil
}
