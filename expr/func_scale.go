package expr

import (
	"fmt"

	"github.com/raintank/metrictank/api/models"
	"gopkg.in/raintank/schema.v1"
)

type FuncScale struct {
	in     Func
	factor float64
}

func NewScale() Func {
	return &FuncScale{}
}

func (s *FuncScale) Signature() ([]arg, []arg) {
	return []arg{
			argSeriesList{val: &s.in},
			argFloat{key: "factor", val: &s.factor},
		}, []arg{
			argSeriesList{},
		}
}

func (s *FuncScale) NeedRange(from, to uint32) (uint32, uint32) {
	return from, to
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
			Target:     fmt.Sprintf("scale(%s,%f)", serie.Target, s.factor),
			Datapoints: out,
			Interval:   serie.Interval,
		}
		outputs = append(outputs, s)
		cache[Req{}] = append(cache[Req{}], s)
	}
	return outputs, nil
}
