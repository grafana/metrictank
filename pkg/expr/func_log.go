package expr

import (
	"fmt"
	"math"

	"github.com/grafana/metrictank/internal/schema"
	"github.com/grafana/metrictank/pkg/api/models"
)

type FuncLog struct {
	in   GraphiteFunc
	base float64
}

func NewLog() GraphiteFunc {
	return &FuncLog{base: 10}
}

func (s *FuncLog) Signature() ([]Arg, []Arg) {
	return []Arg{
			ArgSeriesList{val: &s.in},
			ArgFloat{key: "base", opt: true, validator: []Validator{PositiveButNotOne}, val: &s.base},
		}, []Arg{
			ArgSeriesList{},
		}
}

func (s *FuncLog) Context(context Context) Context {
	return context
}

func (s *FuncLog) Exec(dataMap DataMap) ([]models.Series, error) {
	series, err := s.in.Exec(dataMap)
	if err != nil {
		return nil, err
	}

	outputs := make([]models.Series, 0, len(series))
	baseLog := math.Log(s.base)
	for _, serie := range series {
		out := pointSlicePool.GetMin(len(serie.Datapoints))
		for _, v := range serie.Datapoints {
			// returns NaN for non-positive value.
			val := math.NaN()
			if v.Val > 0 {
				val = math.Log(v.Val) / baseLog
			}
			out = append(out, schema.Point{Val: val, Ts: v.Ts})
		}

		serie.Target = fmt.Sprintf("log(%s,%g)", serie.Target, s.base)
		serie.QueryPatt = fmt.Sprintf("log(%s,%g)", serie.QueryPatt, s.base)
		serie.Tags = serie.CopyTagsWith("log", fmt.Sprintf("%g", s.base))
		serie.Datapoints = out

		outputs = append(outputs, serie)
	}
	dataMap.Add(Req{}, outputs...)
	return outputs, nil
}
