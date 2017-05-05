package expr

import (
	"fmt"
	"math"

	"github.com/raintank/metrictank/api/models"
	"gopkg.in/raintank/schema.v1"
)

type FuncTransformNull struct {
	def float64
}

func NewTransformNull() Func {
	return &FuncTransformNull{math.NaN()}
}

func (s *FuncTransformNull) Signature() ([]arg, []arg) {
	return []arg{
		argSeriesList{},
		argFloat{key: "default", store: &s.def},
	}, []arg{argSeriesList{}}
}

func (s *FuncTransformNull) NeedRange(from, to uint32) (uint32, uint32) {
	return from, to
}

func (s *FuncTransformNull) Exec(cache map[Req][]models.Series, named map[string]interface{}, inputs ...interface{}) ([]interface{}, error) {
	custom := true
	if math.IsNaN(s.def) {
		s.def = 0
		custom = false
	}

	var series []models.Series
	var out []interface{}
	for _, input := range inputs {
		seriesList, ok := input.([]models.Series)
		if !ok {
			break // no more series on input. we hit another parameter
		}
		series = append(series, seriesList...)

	}
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
