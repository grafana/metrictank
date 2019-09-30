package expr

import (
	"fmt"
	"math"

	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/schema"
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

func (s *FuncAbsolute) Exec(cache map[Req][]models.Series) ([]models.Series, error) {
	series, err := s.in.Exec(cache)
	if err != nil {
		return nil, err
	}

	out := make([]models.Series, len(series))
	for i, serie := range series {
		transformed := &out[i]
		transformed.Target = fmt.Sprintf("absolute(%s)", serie.Target)
		transformed.Tags = serie.CopyTagsWith("absolute", "1")
		transformed.Datapoints = pointSlicePool.Get().([]schema.Point)
		transformed.Interval = serie.Interval
		transformed.QueryPatt = fmt.Sprintf("absolute(%s)", serie.QueryPatt)
		transformed.QueryCons = serie.QueryCons
		transformed.Consolidator = serie.Consolidator
		transformed.Meta = serie.Meta

		for _, p := range serie.Datapoints {
			p.Val = math.Abs(p.Val)
			transformed.Datapoints = append(transformed.Datapoints, p)
		}
		cache[Req{}] = append(cache[Req{}], *transformed)
	}

	return out, nil
}
