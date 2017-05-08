package expr

import (
	"fmt"
	"math"

	"github.com/raintank/metrictank/api/models"
	"gopkg.in/raintank/schema.v1"
)

type FuncAvgSeries struct {
	in []models.Series
}

func NewAvgSeries() Func {
	return &FuncAvgSeries{}
}

func (s *FuncAvgSeries) Signature() ([]arg, []arg) {
	return []arg{
		argSeriesLists{},
	}, []arg{argSeries{}}
}

func (s *FuncAvgSeries) NeedRange(from, to uint32) (uint32, uint32) {
	return from, to
}

func (s *FuncAvgSeries) Exec(cache map[Req][]models.Series) ([]interface{}, error) {
	if len(s.in) == 1 {
		s.in[0].Target = fmt.Sprintf("averageSeries(%s)", s.in[0].QueryPatt)
		return []interface{}{s.in[0]}, nil
	}
	out := pointSlicePool.Get().([]schema.Point)
	for i := 0; i < len(s.in[0].Datapoints); i++ {
		num := 0
		sum := float64(0)
		for j := 0; j < len(s.in); j++ {
			p := s.in[j].Datapoints[i].Val
			if !math.IsNaN(p) {
				num++
				sum += p
			}
		}
		point := schema.Point{
			Ts: s.in[0].Datapoints[i].Ts,
		}
		if num == 0 {
			point.Val = math.NaN()
		} else {
			point.Val = sum / float64(num)
		}
		out = append(out, point)
	}
	output := models.Series{
		Target:     fmt.Sprintf("averageSeries(%s)", patternsAsArgs(s.in)),
		Datapoints: out,
		Interval:   s.in[0].Interval,
	}
	cache[Req{}] = append(cache[Req{}], output)

	return []interface{}{output}, nil
}
