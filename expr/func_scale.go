package expr

import (
	"fmt"

	"github.com/grafana/metrictank/api/models"
	"gopkg.in/raintank/schema.v1"
)

type FuncScale struct {
	in     GraphiteFunc
	factor float64
}

func NewScale() GraphiteFunc {
	return &FuncScale{}
}

func (s *FuncScale) Signature() ([]Arg, []Arg) {
	return []Arg{
			ArgSeriesList{val: &s.in},
			ArgFloat{key: "factor", val: &s.factor},
		}, []Arg{
			ArgSeriesList{},
		}
}

func (s *FuncScale) Context(context Context) Context {
	return context
}

func (s *FuncScale) Exec(cache map[Req][]models.Series) ([]models.Series, error) {
	series, err := s.in.Exec(cache)
	if err != nil {
		return nil, err
	}
	var outputs []models.Series
	for _, serie := range series {
		out := pointSlicePool.Get().([]schema.Point)
		for _, v := range serie.Datapoints {
			out = append(out, schema.Point{Val: v.Val * s.factor, Ts: v.Ts})
		}
		s := models.Series{
			Target:       fmt.Sprintf("scale(%s,%f)", serie.Target, s.factor),
			QueryPatt:    fmt.Sprintf("scale(%s,%f)", serie.QueryPatt, s.factor),
			Tags:         serie.Tags,
			Datapoints:   out,
			Interval:     serie.Interval,
			Consolidator: serie.Consolidator,
			QueryCons:    serie.QueryCons,
		}
		outputs = append(outputs, s)
		cache[Req{}] = append(cache[Req{}], s)
	}
	return outputs, nil
}
