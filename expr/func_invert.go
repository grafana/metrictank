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

	for i, serie := range series {
		series[i].Target = fmt.Sprintf("invert(%s)", serie.Target)
		series[i].Tags = serie.CopyTagsWith("invert", "1")
		series[i].QueryPatt = fmt.Sprintf("invert(%s)", serie.QueryPatt)
		series[i].Datapoints = pointSlicePool.Get().([]schema.Point)
		for _, p := range serie.Datapoints {
			if !math.IsNaN(p.Val) && p.Val != 0 {
				p.Val = math.Pow(p.Val, -1)
			} else {
				p.Val = math.NaN()
			}
			series[i].Datapoints = append(series[i].Datapoints, p)
		}
	}

	dataMap.Add(Req{}, series...)
	return series, nil
}
