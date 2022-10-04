package expr

import (
	"fmt"

	"github.com/grafana/metrictank/pkg/api/models"
	"github.com/grafana/metrictank/pkg/schema"
)

type FuncConstantLine struct {
	value float64
	first uint32
	last  uint32
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
	// Graphite implements constantLine in a non-standard way with repspect to time.
	// Normally, graphite is from exclusive, to inclusive, but for constantLine,
	// is from inclusive, to inclusive. This means the first datapoint has a ts 1s
	// earlier than what it would normally be. We do the same here.
	s.first = context.from - 1
	s.last = context.to - 1
	return context
}

func (s *FuncConstantLine) Exec(dataMap DataMap) ([]models.Series, error) {
	out := pointSlicePool.GetMin(3)

	out = append(out, schema.Point{Val: s.value, Ts: s.first})
	diff := s.last - s.first

	// edge cases
	// if first = last - 1, return one datapoint to user, so don't add more points
	// if first = last - 2, return two datapoints where timestamps are first, first +1
	if diff > 2 {
		out = append(out,
			schema.Point{Val: s.value, Ts: s.first + uint32(diff/2.0)},
			schema.Point{Val: s.value, Ts: s.last},
		)
	} else if diff == 2 {
		out = append(out, schema.Point{Val: s.value, Ts: s.first + 1})
	}

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
