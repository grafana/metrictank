package expr

import (
	"fmt"
	"math"

	"github.com/raintank/metrictank/api/models"
	"gopkg.in/raintank/schema.v1"
)

type FuncSumSeries struct {
	in []models.Series
}

func NewSumSeries() Func {
	return &FuncSumSeries{}
}

func (s *FuncSumSeries) Signature() ([]arg, []arg) {
	return []arg{
		argSeriesLists{},
	}, []arg{argSeries{}}
}

func (s *FuncSumSeries) NeedRange(from, to uint32) (uint32, uint32) {
	return from, to
}

func (s *FuncSumSeries) Exec(cache map[Req][]models.Series) ([]interface{}, error) {
	if len(s.in) == 1 {
		s.in[0].Target = fmt.Sprintf("sumSeries(%s)", s.in[0].QueryPatt)
		return []interface{}{s.in[0]}, nil
	}
	out := pointSlicePool.Get().([]schema.Point)
	for i := 0; i < len(s.in[0].Datapoints); i++ {
		nan := true
		point := schema.Point{
			Ts:  s.in[0].Datapoints[i].Ts,
			Val: 0,
		}
		for j := 0; j < len(s.in); j++ {
			if !math.IsNaN(s.in[j].Datapoints[i].Val) {
				point.Val += s.in[j].Datapoints[i].Val
				nan = false
			}
		}
		if nan {
			point.Val = math.NaN()
		}
		out = append(out, point)
	}
	output := models.Series{
		Target:     fmt.Sprintf("sumSeries(%s)", patternsAsArgs(s.in)),
		Datapoints: out,
		Interval:   s.in[0].Interval,
	}
	cache[Req{}] = append(cache[Req{}], output)
	return []interface{}{output}, nil
}
