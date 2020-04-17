package expr

import (
	"fmt"

	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/schema"
)

type FuncMinMax struct {
	in GraphiteFunc
}

func NewMinMax() GraphiteFunc {
	return &FuncMinMax{}
}

func (s *FuncMinMax) Signature() ([]Arg, []Arg) {
	return []Arg{
		ArgSeriesList{val: &s.in}}, []Arg{ArgSeriesList{}}
}

func (s *FuncMinMax) Context(context Context) Context {
	return context
}

func (s *FuncMinMax) Exec(dataMap DataMap) ([]models.Series, error) {
	series, err := s.in.Exec(dataMap)
	if err != nil {
		return nil, err
	}

	outputs := make([]models.Series, len(series))

	for i, serie := range series {
		out := pointSlicePool.Get().([]schema.Point)

		for _, p := range serie.Datapoints {
			out = append(out, schema.Point{Val: p.Val, Ts: p.Ts})
		}

		s := models.Series{
			Target:       fmt.Sprintf("minMax(%s)", serie.Target),
			QueryPatt:    fmt.Sprintf("minMax(%s)", serie.Target),
			Tags:         serie.CopyTagsWith("minMax", "1"),
			Datapoints:   out,
			Interval:     serie.Interval,
			Meta:         serie.Meta,
			QueryMDP:     serie.QueryMDP,
			QueryPNGroup: serie.QueryPNGroup,
		}
		outputs[i] = s
	}
	dataMap.Add(Req{}, outputs...)
	return outputs, nil
}
