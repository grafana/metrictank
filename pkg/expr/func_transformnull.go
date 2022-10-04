package expr

import (
	"fmt"
	"math"

	"github.com/grafana/metrictank/api/models"
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

	output := make([]models.Series, 0, len(series))
	for _, serie := range series {
		out := pointSlicePool.GetMin(len(serie.Datapoints))
		for _, p := range serie.Datapoints {
			if math.IsNaN(p.Val) {
				p.Val = s.def
			}
			out = append(out, p)
		}

		if custom {
			serie.Target = fmt.Sprintf("transformNull(%s,%g)", serie.Target, s.def)
		} else {
			serie.Target = fmt.Sprintf("transformNull(%s)", serie.Target)
		}
		serie.QueryPatt = serie.Target
		serie.Datapoints = out

		output = append(output, serie)
	}
	dataMap.Add(Req{}, output...)
	return output, nil
}
