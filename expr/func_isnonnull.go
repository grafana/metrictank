package expr

import (
	"fmt"
	"math"

	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/schema"
)

type FuncIsNonNull struct {
	in GraphiteFunc
}

func NewIsNonNull() GraphiteFunc {
	return &FuncIsNonNull{}
}

func (s *FuncIsNonNull) Signature() ([]Arg, []Arg) {
	return []Arg{
		ArgSeriesList{val: &s.in}}, []Arg{ArgSeriesList{}}
}

func (s *FuncIsNonNull) Context(context Context) Context {
	return context
}

func (s *FuncIsNonNull) Exec(dataMap DataMap) ([]models.Series, error) {
	series, err := s.in.Exec(dataMap)
	if err != nil {
		return nil, err
	}

	outSeries := make([]models.Series, 0, len(series))
	for _, serie := range series {
		serie.Target = fmt.Sprintf("isNonNull(%s)", serie.Target)
		serie.QueryPatt = fmt.Sprintf("isNonNull(%s)", serie.QueryPatt)
		serie.Tags = serie.CopyTagsWith("isNonNull", "1")

		out := pointSlicePool.Get().([]schema.Point)
		for _, p := range serie.Datapoints {
			if math.IsNaN(p.Val) {
				p.Val = 0
			} else {
				p.Val = 1
			}
			out = append(out, p)
		}

		serie.Datapoints = out
		outSeries = append(outSeries, serie)
	}
	dataMap.Add(Req{}, outSeries...)
	return outSeries, nil
}
