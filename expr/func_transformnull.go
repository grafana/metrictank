package expr

import (
	"fmt"
	"math"

	"github.com/raintank/metrictank/api/models"
	"gopkg.in/raintank/schema.v1"
)

type FuncTransformNull struct {
	in  Func
	def float64
}

func NewTransformNull() Func {
	return &FuncTransformNull{nil, math.NaN()}
}

func (s *FuncTransformNull) Signature() ([]arg, []arg) {
	return []arg{
		argSeriesList{val: &s.in},
		argFloat{key: "default", val: &s.def},
	}, []arg{argSeriesList{}}
}

func (s *FuncTransformNull) NeedRange(from, to uint32) (uint32, uint32) {
	return from, to
}

func (s *FuncTransformNull) Exec(cache map[Req][]models.Series) ([]models.Series, error) {
	series, err := s.in.Exec(cache)
	if err != nil {
		return nil, err
	}
	custom := true
	if math.IsNaN(s.def) {
		s.def = 0
		custom = false
	}

	var out []models.Series
	for _, serie := range series {
		var target string
		if custom {
			target = fmt.Sprintf("transFormNull(%s,%f)", serie.Target, s.def)
		} else {
			target = fmt.Sprintf("transFormNull(%s)", serie.Target)
		}
		transformed := models.Series{
			Target:     target,
			Datapoints: pointSlicePool.Get().([]schema.Point),
			Interval:   serie.Interval,
		}
		for _, p := range serie.Datapoints {
			if math.IsNaN(p.Val) {
				p.Val = s.def
			}
			transformed.Datapoints = append(transformed.Datapoints, p)
		}
		out = append(out, transformed)
		cache[Req{}] = append(cache[Req{}], transformed)
	}
	return out, nil
}
