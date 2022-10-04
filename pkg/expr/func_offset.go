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

	outSeries := make([]models.Series, 0, len(series))
	for _, serie := range series {
		out := pointSlicePool.GetMin(len(serie.Datapoints))
		for _, v := range serie.Datapoints {
			out = append(out, schema.Point{Val: v.Val + s.factor, Ts: v.Ts})
		}
		serie.Target = fmt.Sprintf("offset(%s,%g)", serie.Target, s.factor)
		serie.QueryPatt = fmt.Sprintf("offset(%s,%g)", serie.QueryPatt, s.factor)
		serie.Datapoints = out

		outSeries = append(outSeries, serie)
	}
	dataMap.Add(Req{}, outSeries...)
	return outSeries, nil
}
