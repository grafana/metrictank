package expr

import (
	"fmt"
	"math"

	"github.com/grafana/metrictank/api/models"
	schema "gopkg.in/raintank/schema.v1"
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

func (s *FuncIsNonNull) Exec(cache map[Req][]models.Series) ([]models.Series, error) {
	series, err := s.in.Exec(cache)
	if err != nil {
		return nil, err
	}

	out := make([]models.Series, 0, len(series))
	for _, serie := range series {
		transformed := models.Series{
			Target:       fmt.Sprintf("isNonNull(%s)", serie.Target),
			QueryPatt:    fmt.Sprintf("isNonNull(%s)", serie.QueryPatt),
			Tags:         serie.Tags,
			Datapoints:   pointSlicePool.Get().([]schema.Point),
			Interval:     serie.Interval,
			Consolidator: serie.Consolidator,
			QueryCons:    serie.QueryCons,
		}
		transformed.Tags["isNonNull"] = "1"
		for _, p := range serie.Datapoints {
			if math.IsNaN(p.Val) {
				p.Val = 0
			} else {
				p.Val = 1
			}
			transformed.Datapoints = append(transformed.Datapoints, p)
		}
		out = append(out, transformed)
		cache[Req{}] = append(cache[Req{}], transformed)
	}

	return out, nil
}
