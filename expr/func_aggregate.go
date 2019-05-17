package expr

import (
	"math"

	"github.com/grafana/metrictank/api/models"
	"github.com/raintank/schema"
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
	return context
}

func (s *FuncAggregate) Exec(cache map[Req][]models.Series) ([]models.Series, error) {
	series, _, err := consumeFuncs(cache, s.in)
	if err != nil {
		return nil, err
	}

	agg := seriesAggregator{function: getCrossSeriesAggFunc(s.name), name: s.name}

	output := aggregate(series, agg, s.xFilesFactor)

	cache[Req{}] = append(cache[Req{}], output)

	return []models.Series{output}, nil
}

func aggregate(series []models.Series, agg seriesAggregator, xFilesFactor float64) models.Series {
	if len(series) == 0 {
		return models.Series{}
	}

	if len(series) == 1 {
		name := agg.name + "Series(" + series[0].QueryPatt + ")"
		series[0].Target = name
		series[0].QueryPatt = name
		return series[0]
	}
	out := pointSlicePool.Get().([]schema.Point)

	//remove values in accordance to xFilesFactor
	for i := 0; i < len(series[0].Datapoints); i++ {
		if !crossSeriesXff(series, i, xFilesFactor) {
			for j := 0; j < len(series); j++ {
				series[j].Datapoints[i].Val = math.NaN()
			}
		}
	}
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
	name := agg.name + "Series(" + formatQueryPatts(series) + ")"
	commonTags["aggregatedBy"] = agg.name
	if _, ok := commonTags["name"]; !ok {
		commonTags["name"] = name
	}
	output := models.Series{
		Target:       name,
		QueryPatt:    name,
		Tags:         commonTags,
		Datapoints:   out,
		Interval:     series[0].Interval,
		Consolidator: cons,
		QueryCons:    queryCons,
	}

	return output
}
