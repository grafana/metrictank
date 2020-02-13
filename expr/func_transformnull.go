package expr

import (
	"fmt"
	"math"

	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/schema"
)

type FuncTransformNull struct {
	in  GraphiteFunc
	def float64
}

func NewTransformNull() GraphiteFunc {
	return &FuncTransformNull{nil, math.NaN()}
}

func (s *FuncTransformNull) Signature() ([]Arg, []Arg) {
	return []Arg{
		ArgSeriesList{val: &s.in},
		ArgFloat{key: "default", opt: true, val: &s.def},
	}, []Arg{ArgSeriesList{}}
}

func (s *FuncTransformNull) Context(context Context) Context {
	return context
}

func (s *FuncTransformNull) Exec(dataMap DataMap) ([]models.Series, error) {
	series, err := s.in.Exec(dataMap)
	if err != nil {
		return nil, err
	}
	custom := true
	if math.IsNaN(s.def) {
		s.def = 0
		custom = false
	}

	for i, serie := range series {
		var target string
		if custom {
			target = fmt.Sprintf("transFormNull(%s,%f)", serie.Target, s.def)
		} else {
			target = fmt.Sprintf("transFormNull(%s)", serie.Target)
		}
		series[i].Target = target
		series[i].QueryPatt = target
		series[i].Datapoints = pointSlicePool.Get().([]schema.Point)
		for _, p := range serie.Datapoints {
			if math.IsNaN(p.Val) {
				p.Val = s.def
			}
			series[i].Datapoints = append(series[i].Datapoints, p)
		}
	}
	dataMap.Add(Req{}, series...)
	return series, nil
}
