package expr

import (
	"fmt"
	"math"

	"github.com/raintank/metrictank/api/models"
	"gopkg.in/raintank/schema.v1"
)

type FuncSumSeries struct {
	in []Func
}

func NewSumSeries() Func {
	return &FuncSumSeries{}
}

func (s *FuncSumSeries) Signature() ([]Arg, []Arg) {
	return []Arg{
		ArgSeriesLists{val: &s.in},
	}, []Arg{ArgSeries{}}
}

func (s *FuncSumSeries) NeedRange(from, to uint32) (uint32, uint32) {
	return from, to
}

func (s *FuncSumSeries) Exec(cache map[Req][]models.Series) ([]models.Series, error) {
	var series []models.Series
	for i := range s.in {
		in, err := s.in[i].Exec(cache)
		if err != nil {
			return nil, err
		}
		series = append(series, in...)
	}

	if len(series) == 1 {
		series[0].Target = fmt.Sprintf("sumSeries(%s)", series[0].QueryPatt)
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
				point.Val += series[j].Datapoints[i].Val
				nan = false
			}
		}
		if nan {
			point.Val = math.NaN()
		}
		out = append(out, point)
	}
	output := models.Series{
		Target:     fmt.Sprintf("sumSeries(%s)", patternsAsArgs(series)),
		Datapoints: out,
		Interval:   series[0].Interval,
	}
	cache[Req{}] = append(cache[Req{}], output)
	return []models.Series{output}, nil
}
