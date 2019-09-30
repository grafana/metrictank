package expr

import (
	"fmt"
	"math"

	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/schema"
)

type FuncNonNegativeDerivative struct {
	in       GraphiteFunc
	maxValue float64
}

func NewNonNegativeDerivative() GraphiteFunc {
	return &FuncNonNegativeDerivative{maxValue: math.NaN()}
}

func (s *FuncNonNegativeDerivative) Signature() ([]Arg, []Arg) {
	return []Arg{
		ArgSeriesList{val: &s.in},
		ArgFloat{
			key: "maxValue",
			opt: true,
			val: &s.maxValue}}, []Arg{ArgSeriesList{}}
}

func (s *FuncNonNegativeDerivative) Context(context Context) Context {
	return context
}

func (s *FuncNonNegativeDerivative) Exec(cache map[Req][]models.Series) ([]models.Series, error) {
	series, err := s.in.Exec(cache)
	if err != nil {
		return nil, err
	}

	outSeries := make([]models.Series, len(series))
	for i, in := range series {
		serie := in.CopyBare()
		serie.Target = fmt.Sprintf("nonNegativeDerivative(%s)", in.Target)
		serie.QueryPatt = fmt.Sprintf("nonNegativeDerivative(%s)", in.QueryPatt)
		serie.Tags = serie.CopyTagsWith("nonNegativeDerivative", "1")
		serie.Meta = in.Meta
		out := pointSlicePool.Get().([]schema.Point)

		prev := math.NaN()
		for _, p := range serie.Datapoints {
			var delta float64
			delta, prev = nonNegativeDelta(p.Val, prev, s.maxValue)
			p.Val = delta
			out = append(out, p)
		}
		serie.Datapoints = out
		outSeries[i] = serie
	}
	cache[Req{}] = append(cache[Req{}], outSeries...)
	return outSeries, nil
}

func nonNegativeDelta(val, prev, maxValue float64) (float64, float64) {
	if val > maxValue {
		return math.NaN(), math.NaN()
	}

	if math.IsNaN(prev) || math.IsNaN(val) {
		return math.NaN(), val
	}

	if val >= prev {
		return val - prev, val
	}

	if !math.IsNaN(maxValue) {
		return maxValue + 1 + val - prev, val
	}

	return math.NaN(), val
}
