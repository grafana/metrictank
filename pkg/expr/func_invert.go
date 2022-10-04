package expr

import (
	"fmt"
	"math"

	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/schema"
)

type FuncInvert struct {
	in GraphiteFunc
}

func NewInvert() GraphiteFunc {
	return &FuncInvert{}
}

func (s *FuncInvert) Signature() ([]Arg, []Arg) {
	return []Arg{
		ArgSeriesList{val: &s.in}}, []Arg{ArgSeriesList{}}
}

func (s *FuncInvert) Context(context Context) Context {
	return context
}

func (s *FuncInvert) Exec(dataMap DataMap) ([]models.Series, error) {
	series, err := s.in.Exec(dataMap)
	if err != nil {
		return nil, err
	}

	outputs := make([]models.Series, len(series))

	newVal := func(oldVal float64) float64 {
		if !math.IsNaN(oldVal) && oldVal != 0 {
			return math.Pow(oldVal, -1)
		}
		return math.NaN()
	}

	for i, serie := range series {
		out := pointSlicePool.GetMin(len(serie.Datapoints))

		for _, p := range serie.Datapoints {
			out = append(out, schema.Point{Val: newVal(p.Val), Ts: p.Ts})
		}
		s := models.Series{
			Target:       fmt.Sprintf("invert(%s)", serie.Target),
			QueryPatt:    fmt.Sprintf("invert(%s)", serie.QueryPatt),
			Tags:         serie.CopyTagsWith("invert", "1"),
			Datapoints:   out,
			Interval:     serie.Interval,
			Meta:         serie.Meta,
			QueryMDP:     serie.QueryMDP,
			QueryPNGroup: serie.QueryPNGroup,
			QueryFrom:    serie.QueryFrom,
			QueryTo:      serie.QueryTo,
		}

		outputs[i] = s
	}

	dataMap.Add(Req{}, outputs...)
	return outputs, nil
}
