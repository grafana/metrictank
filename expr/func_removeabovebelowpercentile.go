package expr

import (
	"errors"
	"fmt"
	"math"
	"sort"

	schema "gopkg.in/raintank/schema.v1"

	"github.com/grafana/metrictank/api/models"
)

type FuncRemoveAboveBelowPercentile struct {
	in    GraphiteFunc
	n     float64
	above bool
}

func NewRemoveAboveBelowPercentileConstructor(above bool) func() GraphiteFunc {
	return func() GraphiteFunc {
		return &FuncRemoveAboveBelowPercentile{above: above}
	}
}

func (s *FuncRemoveAboveBelowPercentile) Signature() ([]Arg, []Arg) {

	return []Arg{
		ArgSeriesList{val: &s.in},
		ArgFloat{key: "n", val: &s.n},
	}, []Arg{ArgSeriesList{}}
}

func (s *FuncRemoveAboveBelowPercentile) Context(context Context) Context {
	return context
}

func (s *FuncRemoveAboveBelowPercentile) Exec(cache map[Req][]models.Series) ([]models.Series, error) {
	series, err := s.in.Exec(cache)
	if err != nil {
		return nil, err
	}

	if s.n <= 0 {
		return nil, errors.New("The requested percent is required to be greater than 0")
	}

	if len(series) == 0 {
		return series, nil
	}

	var output []models.Series
	sortedDatapointVals := make([]float64, 0, len(series[0].Datapoints)) //reuse float64 slice
	for _, serie := range series {
		if s.above {
			serie.Target = fmt.Sprintf("removeAbovePercentile(%s, %g)", serie.Target, s.n)
		} else {
			serie.Target = fmt.Sprintf("removeBelowPercentile(%s, %g)", serie.Target, s.n)
		}
		serie.QueryPatt = serie.Target

		newTags := make(map[string]string, len(serie.Tags)+1)
		for k, v := range serie.Tags {
			newTags[k] = v
		}
		newTags["nPercentile"] = fmt.Sprintf("%g", s.n)
		serie.Tags = newTags

		percentile := getPercentileValue(serie.Datapoints, s.n, sortedDatapointVals)

		out := pointSlicePool.Get().([]schema.Point)
		for _, p := range serie.Datapoints {
			if s.above {
				if p.Val > percentile {
					p.Val = math.NaN()
				}
			} else {
				if p.Val < percentile {
					p.Val = math.NaN()
				}
			}
			out = append(out, p)
		}
		serie.Datapoints = out
		output = append(output, serie)
	}

	cache[Req{}] = append(cache[Req{}], output...)

	return output, nil
}

func getPercentileValue(datapoints []schema.Point, n float64, sortedDatapointVals []float64) float64 {
	sortedDatapointVals = sortedDatapointVals[:0]
	for _, p := range datapoints {
		if !math.IsNaN(p.Val) {
			sortedDatapointVals = append(sortedDatapointVals, p.Val)
		}
	}

	sort.Float64s(sortedDatapointVals)

	index := math.Min(math.Ceil(n/100.0*float64(len(sortedDatapointVals)+1)), float64(len(sortedDatapointVals))) - 1

	return sortedDatapointVals[int(index)]
}
