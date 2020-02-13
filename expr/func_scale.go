package expr

import (
	"fmt"

	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/schema"
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
	for i, serie := range series {
		out := pointSlicePool.Get().([]schema.Point)
		for _, v := range serie.Datapoints {
			out = append(out, schema.Point{Val: v.Val * s.factor, Ts: v.Ts})
		}
		series[i].Target = fmt.Sprintf("scale(%s,%f)", serie.Target, s.factor)
		series[i].QueryPatt = fmt.Sprintf("scale(%s,%f)", serie.QueryPatt, s.factor)
		series[i].Datapoints = out
	}
	dataMap.Add(Req{}, series...)
	return series, nil
}
