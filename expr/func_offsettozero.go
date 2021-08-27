package expr

import (
	"fmt"
	"github.com/grafana/metrictank/schema"
	"math"

	"github.com/grafana/metrictank/api/models"
)

type FuncOffsetToZero struct {
	in GraphiteFunc
}

func NewOffsetToZero() GraphiteFunc {
	return &FuncOffsetToZero{}
}

func (s *FuncOffsetToZero) Signature() ([]Arg, []Arg) {
	return []Arg{
			ArgSeriesList{val: &s.in},
		},
		[]Arg{ArgSeriesList{}}
}

func (s *FuncOffsetToZero) Context(context Context) Context {
	return context
}

func (s *FuncOffsetToZero) Exec(dataMap DataMap) ([]models.Series, error) {
	series, err := s.in.Exec(dataMap)
	if err != nil {
		return nil, err
	}

	outSeries := make([]models.Series, 0, len(series))
	for _, serie := range series {
		serie.Target = fmt.Sprintf("offsetToZero(%s)", serie.Target)
		serie.Tags = serie.CopyTagsWith("offsetToZero", "1")
		serie.QueryPatt = fmt.Sprintf("offsetToZero(%s)", serie.QueryPatt)
		out := pointSlicePool.Get()
		var min = math.Inf(1)

		for _, p := range serie.Datapoints {
			if math.IsInf(p.Val, 0) || math.IsNaN(p.Val) {
				continue
			}
			min = math.Min(min, p.Val)
		}
		for _, p := range serie.Datapoints {
			out = append(out, schema.Point{Val: p.Val - min, Ts: p.Ts})
		}

		serie.Datapoints = out
		outSeries = append(outSeries, serie)
	}
	dataMap.Add(Req{}, outSeries...)
	return outSeries, nil
}
