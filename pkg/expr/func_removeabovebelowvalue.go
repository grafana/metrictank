package expr

import (
	"fmt"
	"math"

	"github.com/grafana/metrictank/pkg/api/models"
)

type FuncRemoveAboveBelowValue struct {
	in    GraphiteFunc
	n     float64
	above bool
}

func NewRemoveAboveBelowValueConstructor(above bool) func() GraphiteFunc {
	return func() GraphiteFunc {
		return &FuncRemoveAboveBelowValue{above: above}
	}
}

func (s *FuncRemoveAboveBelowValue) Signature() ([]Arg, []Arg) {

	return []Arg{
		ArgSeriesList{val: &s.in},
		ArgFloat{key: "n", val: &s.n},
	}, []Arg{ArgSeriesList{}}
}

func (s *FuncRemoveAboveBelowValue) Context(context Context) Context {
	return context
}

func (s *FuncRemoveAboveBelowValue) Exec(dataMap DataMap) ([]models.Series, error) {
	series, err := s.in.Exec(dataMap)
	if err != nil {
		return nil, err
	}

	var output []models.Series
	for _, serie := range series {
		if s.above {
			serie.Target = fmt.Sprintf("removeAboveValue(%s, %g)", serie.Target, s.n)
		} else {
			serie.Target = fmt.Sprintf("removeBelowValue(%s, %g)", serie.Target, s.n)
		}
		serie.QueryPatt = serie.Target

		out := pointSlicePool.GetMin(len(serie.Datapoints))
		for _, p := range serie.Datapoints {
			if s.above {
				if p.Val > s.n {
					p.Val = math.NaN()
				}
			} else {
				if p.Val < s.n {
					p.Val = math.NaN()
				}
			}
			out = append(out, p)
		}
		serie.Datapoints = out
		output = append(output, serie)
	}

	dataMap.Add(Req{}, output...)

	return output, nil
}
