package expr

import (
	"fmt"
	"math"

	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/consolidation"
	"github.com/grafana/metrictank/schema"
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

	for i, serie := range series {
		series[i].Target = fmt.Sprintf("derivative(%s)", serie.Target)
		series[i].Tags = serie.CopyTagsWith("derivative", "1")
		series[i].QueryPatt = fmt.Sprintf("derivative(%s)", serie.QueryPatt)
		series[i].Consolidator = consolidation.None
		series[i].QueryCons = consolidation.None
		out := pointSlicePool.Get().([]schema.Point)

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
		series[i].Datapoints = out
	}
	dataMap.Add(Req{}, series...)
	return series, nil
}
