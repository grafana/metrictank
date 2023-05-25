package expr

import (
	"fmt"
	"math"

	"github.com/grafana/metrictank/internal/consolidation"
	"github.com/grafana/metrictank/pkg/api/models"
)

type FuncDerivative struct {
	in GraphiteFunc
}

func NewDerivative() GraphiteFunc {
	return &FuncDerivative{}
}

func (s *FuncDerivative) Signature() ([]Arg, []Arg) {
	return []Arg{
		ArgSeriesList{val: &s.in}}, []Arg{ArgSeriesList{}}
}

func (s *FuncDerivative) Context(context Context) Context {
	return context
}

func (s *FuncDerivative) Exec(dataMap DataMap) ([]models.Series, error) {
	series, err := s.in.Exec(dataMap)
	if err != nil {
		return nil, err
	}

	outSeries := make([]models.Series, 0, len(series))
	for _, serie := range series {
		serie.Target = fmt.Sprintf("derivative(%s)", serie.Target)
		serie.Tags = serie.CopyTagsWith("derivative", "1")
		serie.QueryPatt = fmt.Sprintf("derivative(%s)", serie.QueryPatt)
		serie.Consolidator = consolidation.None
		serie.QueryCons = consolidation.None
		out := pointSlicePool.GetMin(len(serie.Datapoints))

		prev := math.NaN()
		for _, p := range serie.Datapoints {
			val := p.Val
			if math.IsNaN(prev) || math.IsNaN(val) {
				p.Val = math.NaN()
			} else {
				p.Val -= prev
			}
			prev = val
			out = append(out, p)
		}
		serie.Datapoints = out

		outSeries = append(outSeries, serie)
	}
	dataMap.Add(Req{}, outSeries...)
	return outSeries, nil
}
