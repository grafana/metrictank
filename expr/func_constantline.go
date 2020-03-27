package expr

import (
	"fmt"

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
	s.from = context.from - 1
	s.to = context.to - 1
	return context
}

func (s *FuncConstantLine) Exec(dataMap DataMap) ([]models.Series, error) {
	out := pointSlicePool.Get().([]schema.Point)
	out = append(out,
		schema.Point{Val: s.value, Ts: s.from},
		schema.Point{Val: s.value, Ts: s.from + uint32((s.to-s.from)/2.0)},
		schema.Point{Val: s.value, Ts: s.to},
	)

	strValue := fmt.Sprintf("%g", s.value)

	outputs := make([]models.Series, 1)
	outputs[0] = models.Series{
		Target:     strValue,
		QueryPatt:  strValue,
		Datapoints: out,
	}
	outputs[0].SetTags()

	dataMap.Add(Req{}, outputs...)
	return outputs, nil
}
