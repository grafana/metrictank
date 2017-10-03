package expr

import (
	"fmt"
	"math"
	"strings"

	"github.com/grafana/metrictank/api/models"
	"gopkg.in/raintank/schema.v1"
)

type FuncMaxSeries struct {
	in []GraphiteFunc
}

func NewMaxSeries() GraphiteFunc {
	return &FuncMaxSeries{}
}

func (s *FuncMaxSeries) Signature() ([]Arg, []Arg) {
	return []Arg{
		ArgSeriesLists{val: &s.in},
	}, []Arg{ArgSeries{}}
}

func (s *FuncMaxSeries) Context(context Context) Context {
	return context
}

func (s *FuncMaxSeries) Exec(cache map[Req][]models.Series) ([]models.Series, error) {
	series, queryPatts, err := consumeFuncs(cache, s.in)
	if err != nil {
		return nil, err
	}

	if len(series) == 0 {
		return series, nil
	}

	if len(series) == 1 {
		name := fmt.Sprintf("maxSeries(%s)", series[0].QueryPatt)
		series[0].Target = name
		series[0].QueryPatt = name
		return series, nil
	}
	out := pointSlicePool.Get().([]schema.Point)
	for i := 0; i < len(series[0].Datapoints); i++ {
		nan := true
		point := schema.Point{
			Ts:  series[0].Datapoints[i].Ts,
			Val: 0,
		}
		for j := 0; j < len(series); j++ {
			if !math.IsNaN(series[j].Datapoints[i].Val) {
				point.Val = math.Max(point.Val, series[j].Datapoints[i].Val)
				nan = false
			}
		}
		if nan {
			point.Val = math.NaN()
		}
		out = append(out, point)
	}
	name := fmt.Sprintf("maxSeries(%s)", strings.Join(queryPatts, ","))
	cons, queryCons := summarizeCons(series)
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
