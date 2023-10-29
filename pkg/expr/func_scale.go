package expr

import (
	"fmt"

	"github.com/grafana/metrictank/internal/schema"
	"github.com/grafana/metrictank/pkg/api/models"
)

type FuncScale struct {
	in     GraphiteFunc
	factor float64
}

func NewScale() GraphiteFunc {
	return &FuncScale{}
}

func (s *FuncScale) Signature() ([]Arg, []Arg) {
	return []Arg{
			ArgSeriesList{val: &s.in},
			ArgFloat{key: "factor", val: &s.factor},
		}, []Arg{
			ArgSeriesList{},
		}
}

func (s *FuncScale) Context(context Context) Context {
	return context
}

func (s *FuncScale) Exec(dataMap DataMap) ([]models.Series, error) {
	series, err := s.in.Exec(dataMap)
	if err != nil {
		return nil, err
	}

	output := make([]models.Series, 0, len(series))
	for _, serie := range series {
		out := pointSlicePool.GetMin(len(serie.Datapoints))
		for _, v := range serie.Datapoints {
			out = append(out, schema.Point{Val: v.Val * s.factor, Ts: v.Ts})
		}

		serie.Target = fmt.Sprintf("scale(%s,%g)", serie.Target, s.factor)
		serie.QueryPatt = fmt.Sprintf("scale(%s,%g)", serie.QueryPatt, s.factor)
		serie.Datapoints = out

		output = append(output, serie)
	}
	dataMap.Add(Req{}, output...)
	return output, nil
}
