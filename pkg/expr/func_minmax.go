package expr

import (
	"fmt"
	"math"

	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/batch"
	"github.com/grafana/metrictank/schema"
)

type FuncMinMax struct {
	in GraphiteFunc
}

func NewMinMax() GraphiteFunc {
	return &FuncMinMax{}
}

func (s *FuncMinMax) Signature() ([]Arg, []Arg) {
	return []Arg{
		ArgSeriesList{val: &s.in}}, []Arg{ArgSeriesList{}}
}

func (s *FuncMinMax) Context(context Context) Context {
	return context
}

func (s *FuncMinMax) Exec(dataMap DataMap) ([]models.Series, error) {
	series, err := s.in.Exec(dataMap)
	if err != nil {
		return nil, err
	}

	valOrDefault := func(val float64, def float64) float64 {
		if math.IsNaN(val) {
			return def
		}
		return val
	}

	minMax := func(val float64, min float64, max float64) float64 {
		if math.IsNaN(val) {
			return val
		}
		if max-min == 0 {
			return 0
		}
		return (val - min) / (max - min)
	}

	outputs := make([]models.Series, 0, len(series))

	for _, serie := range series {
		out := pointSlicePool.GetMin(len(serie.Datapoints))

		minVal := valOrDefault(batch.Min(serie.Datapoints), 0)
		maxVal := valOrDefault(batch.Max(serie.Datapoints), 0)

		for _, p := range serie.Datapoints {
			out = append(out, schema.Point{Val: minMax(p.Val, minVal, maxVal), Ts: p.Ts})
		}

		serie.Target = fmt.Sprintf("minMax(%s)", serie.Target)
		serie.QueryPatt = fmt.Sprintf("minMax(%s)", serie.QueryPatt)
		serie.Tags = serie.CopyTagsWith("minMax", "1")
		serie.Datapoints = out

		outputs = append(outputs, serie)
	}
	dataMap.Add(Req{}, outputs...)
	return outputs, nil
}
