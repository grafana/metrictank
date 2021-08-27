package expr

import (
	"fmt"

	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/batch"
	"github.com/grafana/metrictank/schema"
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
		out := pointSlicePool.GetMin(len(serie.Datapoints))

		min := batch.Min(serie.Datapoints)
		for _, p := range serie.Datapoints {
			out = append(out, schema.Point{Val: p.Val - min, Ts: p.Ts})
		}

		serie.Datapoints = out
		outSeries = append(outSeries, serie)
	}
	dataMap.Add(Req{}, outSeries...)
	return outSeries, nil
}
