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

	out := make([]models.Series, len(series))
	for i, serie := range series {
		transformed := &out[i]
		transformed.Target = fmt.Sprintf("integral(%s)", serie.Target)
		transformed.Tags = serie.CopyTagsWith("integral", "1")
		transformed.Datapoints = pointSlicePool.Get().([]schema.Point)
		transformed.QueryPatt = fmt.Sprintf("integral(%s)", serie.QueryPatt)
		transformed.Interval = serie.Interval
		transformed.Consolidator = serie.Consolidator
		transformed.QueryCons = serie.QueryCons
		transformed.Meta = serie.Meta

		current := 0.0
		for _, p := range serie.Datapoints {
			if !math.IsNaN(p.Val) {
				current += p.Val
				p.Val = current
			}
			transformed.Datapoints = append(transformed.Datapoints, p)
		}
		cache[Req{}] = append(cache[Req{}], *transformed)
	}

	return out, nil
}
