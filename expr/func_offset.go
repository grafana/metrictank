package expr

import (
	"fmt"

	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/schema"
)

type FuncOffset struct {
	in     GraphiteFunc
	factor float64
}

func NewOffset() GraphiteFunc {
	return &FuncOffset{}
}

func (s *FuncOffset) Signature() ([]Arg, []Arg) {
	return []Arg{
			ArgSeriesList{val: &s.in},
			ArgFloat{key: "factor", val: &s.factor},
		}, []Arg{
			ArgSeriesList{},
		}
}

func (s *FuncOffset) Context(context Context) Context {
	return context
}

func (s *FuncOffset) Exec(dataMap DataMap) ([]models.Series, error) {
	series, err := s.in.Exec(dataMap)
	if err != nil {
		return nil, err
	}
	for i, serie := range series {
		out := pointSlicePool.Get().([]schema.Point)
		for _, v := range serie.Datapoints {
			out = append(out, schema.Point{Val: v.Val + s.factor, Ts: v.Ts})
		}
		series[i].Target = fmt.Sprintf("offset(%s,%f)", serie.Target, s.factor)
		series[i].QueryPatt = fmt.Sprintf("offset(%s,%f)", serie.QueryPatt, s.factor)
		series[i].Datapoints = out
	}
	dataMap.Add(Req{}, series...)
	return series, nil
}
