package expr

import (
	"fmt"
	"math"

	"github.com/grafana/metrictank/api/models"
	"github.com/raintank/schema"
)

type FuncDerivative struct {
	in GraphiteFunc
}

func NewDerivative() GraphiteFunc {
	return &FuncDerivative{}
}

func (s *FuncDerivative) Signature() ([]Arg, []Arg) {
	return []Arg{
		ArgSeriesList{val: &s.in}}, []Arg{ArgSeriesList{}}
}

func (s *FuncDerivative) Context(context Context) Context {
	return context
}

func (s *FuncDerivative) Exec(cache map[Req][]models.Series) ([]models.Series, error) {
	series, err := s.in.Exec(cache)
	if err != nil {
		return nil, err
	}

	outSeries := make([]models.Series, len(series))
	for i, serie := range series {
		serie.Target = fmt.Sprintf("derivative(%s)", serie.Target)
		serie.QueryPatt = fmt.Sprintf("derivative(%s)", serie.QueryPatt)
		out := pointSlicePool.Get().([]schema.Point)

		newTags := make(map[string]string, len(serie.Tags)+1)
		for k, v := range serie.Tags {
			newTags[k] = v
		}
		newTags["derivative"] = "1"
		serie.Tags = newTags

		prev := math.NaN()
		for _, p := range serie.Datapoints {
			val := p.Val
			if math.IsNaN(prev) || math.IsNaN(val) {
				p.Val = math.NaN()
			} else {
				p.Val -= prev
			}
			prev = val
			out = append(out, p)
		}
		serie.Datapoints = out
		outSeries[i] = serie
	}
	cache[Req{}] = append(cache[Req{}], outSeries...)
	return outSeries, nil
}
