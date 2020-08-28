package expr

import (
	"fmt"
	"math"

	"github.com/grafana/metrictank/api/models"
)

type FuncAbsolute struct {
	in GraphiteFunc
}

func NewAbsolute() GraphiteFunc {
	return &FuncAbsolute{}
}

func (s *FuncAbsolute) Signature() ([]Arg, []Arg) {
	return []Arg{
		ArgSeriesList{val: &s.in}}, []Arg{ArgSeriesList{}}
}

func (s *FuncAbsolute) Context(context Context) Context {
	return context
}

func (s *FuncAbsolute) Exec(dataMap DataMap) ([]models.Series, error) {
	series, err := s.in.Exec(dataMap)
	if err != nil {
		return nil, err
	}

	outSeries := make([]models.Series, 0, len(series))
	for _, serie := range series {
		serie.Target = fmt.Sprintf("absolute(%s)", serie.Target)
		serie.Tags = serie.CopyTagsWith("absolute", "1")
		serie.QueryPatt = fmt.Sprintf("absolute(%s)", serie.QueryPatt)
		out := pointSlicePool.Get()
		for _, p := range serie.Datapoints {
			p.Val = math.Abs(p.Val)
			out = append(out, p)
		}
		serie.Datapoints = out
		outSeries = append(outSeries, serie)
	}
	dataMap.Add(Req{}, outSeries...)
	return outSeries, nil
}
