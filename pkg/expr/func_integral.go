package expr

import (
	"fmt"
	"math"

	"github.com/grafana/metrictank/pkg/api/models"
	"github.com/grafana/metrictank/pkg/consolidation"
)

type FuncIntegral struct {
	in GraphiteFunc
}

func NewIntegral() GraphiteFunc {
	return &FuncIntegral{}
}

func (s *FuncIntegral) Signature() ([]Arg, []Arg) {
	return []Arg{
		ArgSeriesList{val: &s.in}}, []Arg{ArgSeriesList{}}
}

func (s *FuncIntegral) Context(context Context) Context {
	return context
}

func (s *FuncIntegral) Exec(dataMap DataMap) ([]models.Series, error) {
	series, err := s.in.Exec(dataMap)
	if err != nil {
		return nil, err
	}

	outSeries := make([]models.Series, 0, len(series))
	for _, serie := range series {
		serie.Target = fmt.Sprintf("integral(%s)", serie.Target)
		serie.Tags = serie.CopyTagsWith("integral", "1")
		serie.QueryPatt = fmt.Sprintf("integral(%s)", serie.QueryPatt)
		serie.Consolidator = consolidation.None
		serie.QueryCons = consolidation.None

		current := 0.0

		out := pointSlicePool.GetMin(len(serie.Datapoints))
		for _, p := range serie.Datapoints {
			if !math.IsNaN(p.Val) {
				current += p.Val
				p.Val = current
			}
			out = append(out, p)
		}
		serie.Datapoints = out
		outSeries = append(outSeries, serie)

	}
	dataMap.Add(Req{}, outSeries...)
	return outSeries, nil
}
