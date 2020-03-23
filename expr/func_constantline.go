package expr

import (
	"strconv"

	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/schema"
)

type FuncConstantLine struct {
	value float64
	from  uint32
	to    uint32
}

func NewConstantLine() GraphiteFunc {
	return &FuncConstantLine{}
}

func (s *FuncConstantLine) Signature() ([]Arg, []Arg) {
	return []Arg{
			ArgFloat{key: "value", val: &s.value},
		}, []Arg{
			ArgSeriesList{},
		}
}

func (s *FuncConstantLine) Context(context Context) Context {
	s.from = context.from
	s.to = context.to
	return context
}

func (s *FuncConstantLine) Exec(dataMap DataMap) ([]models.Series, error) {
	out := pointSlicePool.Get().([]schema.Point)
	out = append(out,
			schema.Point{Val: s.value, Ts: s.from},
			schema.Point{Val: s.value, Ts: s.from + uint32((s.to-s.from)/2.0)},
			schema.Point{Val: s.value, Ts: s.to},
		)

	strValue := strconv.FormatFloat(s.value, 'f', -1, 64)

	outputs := make([]models.Series, 1)
	outputs[0] = models.Series{
		Target: strValue,
		QueryPatt: strValue,
		Datapoints: out,
	}

	dataMap.Add(Req{}, outputs...)
	return outputs, nil
}
