package expr

import (
	"fmt"
	"math"

	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/schema"
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

func (s *FuncIntegral) Exec(cache map[Req][]models.Series) ([]models.Series, error) {
	series, err := s.in.Exec(cache)
	if err != nil {
		return nil, err
	}

	for i, serie := range series {
		series[i].Target = fmt.Sprintf("integral(%s)", serie.Target)
		series[i].Tags = serie.CopyTagsWith("integral", "1")
		series[i].QueryPatt = fmt.Sprintf("integral(%s)", serie.QueryPatt)
		series[i].Datapoints = pointSlicePool.Get().([]schema.Point)

		current := 0.0
		for _, p := range serie.Datapoints {
			if !math.IsNaN(p.Val) {
				current += p.Val
				p.Val = current
			}
			series[i].Datapoints = append(series[i].Datapoints, p)
		}
	}
	cache[Req{}] = append(cache[Req{}], series...)
	return series, nil
}
