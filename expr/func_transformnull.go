package expr

import (
	"fmt"
	"math"

	"github.com/raintank/metrictank/api/models"
	"gopkg.in/raintank/schema.v1"
)

type FuncTransformNull struct {
	def      float64
	explicit bool
}

func NewTransformNull() Func {
	return &FuncTransformNull{}
}

func (s *FuncTransformNull) Signature() ([]argType, []optArg, []argType) {
	return []argType{seriesList}, []optArg{{"default", float}}, []argType{series}
}

func (s *FuncTransformNull) Init(args []*expr, kwargs map[string]*expr) error {
	lastArg := args[len(args)-1]
	if lastArg.etype != etFunc && lastArg.etype != etName {
		s.def = lastArg.float
		s.explicit = true
	}
	if a, ok := kwargs["default"]; ok {
		s.def = a.float
		s.explicit = true
	}
	return nil
}

func (s *FuncTransformNull) NeedRange(from, to uint32) (uint32, uint32) {
	return from, to
}

func (s *FuncTransformNull) Exec(cache map[Req][]models.Series, named map[string]interface{}, inputs ...interface{}) ([]interface{}, error) {
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
		if s.explicit {
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
