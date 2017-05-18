package expr

import (
	"fmt"
	"math"

	"github.com/raintank/metrictank/api/models"
	"gopkg.in/raintank/schema.v1"
)

type FuncAvgSeries struct {
	in []GraphiteFunc
}

func NewAvgSeries() GraphiteFunc {
	return &FuncAvgSeries{}
}

func (s *FuncAvgSeries) Signature() ([]Arg, []Arg) {
	return []Arg{
		ArgSeriesLists{val: &s.in},
	}, []Arg{ArgSeries{}}
}

func (s *FuncAvgSeries) Context(context Context) Context {
	return context
}

func (s *FuncAvgSeries) Exec(cache map[Req][]models.Series) ([]models.Series, error) {
	var series []models.Series
	for i := range s.in {
		in, err := s.in[i].Exec(cache)
		if err != nil {
			return nil, err
		}
		series = append(series, in...)
	}
	if len(series) == 1 {
		series[0].Target = fmt.Sprintf("averageSeries(%s)", series[0].QueryPatt)
		return series, nil
	}
	out := pointSlicePool.Get().([]schema.Point)
	for i := 0; i < len(series[0].Datapoints); i++ {
		num := 0
		sum := float64(0)
		for j := 0; j < len(series); j++ {
			p := series[j].Datapoints[i].Val
			if !math.IsNaN(p) {
				num++
				sum += p
			}
		}
		point := schema.Point{
			Ts: series[0].Datapoints[i].Ts,
		}
		if num == 0 {
			point.Val = math.NaN()
		} else {
			point.Val = sum / float64(num)
		}
		out = append(out, point)
	}

	cons, queryCons := summarizeCons(series)
	output := models.Series{
		Target:       fmt.Sprintf("averageSeries(%s)", patternsAsArgs(series)),
		Datapoints:   out,
		Interval:     series[0].Interval,
		Consolidator: cons,
		QueryCons:    queryCons,
	}
	cache[Req{}] = append(cache[Req{}], output)

	return []models.Series{output}, nil
}
