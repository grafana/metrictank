package expr

import (
	"fmt"
	"math"

	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/schema"
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
			ArgFloat{key: "base", opt: true, validator: []Validator{FloatPositiveNotOne}, val: &s.base},
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
	for _, serie := range series {
		out := pointSlicePool.GetMin(len(serie.Datapoints))

		for _, v := range serie.Datapoints {
			// math.Log returns NaN for negative value and -Inf() for 0 value.
			out = append(out, schema.Point{Val: math.Log(v.Val) / math.Log(s.base), Ts: v.Ts})
		}

		serie.Target = fmt.Sprintf("log(%s,%g)", serie.Target, s.base)
		serie.QueryPatt = fmt.Sprintf("log(%s,%g)", serie.QueryPatt, s.base)
		serie.Datapoints = out

		outputs = append(outputs, serie)
	}
	dataMap.Add(Req{}, outputs...)
	return outputs, nil
}
