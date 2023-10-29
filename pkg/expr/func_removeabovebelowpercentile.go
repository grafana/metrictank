package expr

import (
	"fmt"
	"math"
	"sort"

	"github.com/grafana/metrictank/internal/schema"

	"github.com/grafana/metrictank/pkg/api/models"
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
		ArgFloat{key: "n", val: &s.n, validator: []Validator{NonNegativePercent}},
	}, []Arg{ArgSeriesList{}}
}

func (s *FuncRemoveAboveBelowPercentile) Context(context Context) Context {
	return context
}

func (s *FuncRemoveAboveBelowPercentile) Exec(dataMap DataMap) ([]models.Series, error) {
	series, err := s.in.Exec(dataMap)
	if err != nil {
		return nil, err
	}

	if len(series) == 0 {
		return series, nil
	}

	var output []models.Series

	// will be reused for each getPercentileValue call
	sortedDatapointVals := make([]float64, 0, len(series[0].Datapoints))
	for _, serie := range series {
		if s.above {
			serie.Target = fmt.Sprintf("removeAbovePercentile(%s, %g)", serie.Target, s.n)
		} else {
			serie.Target = fmt.Sprintf("removeBelowPercentile(%s, %g)", serie.Target, s.n)
		}
		serie.QueryPatt = serie.Target
		serie.Tags = serie.CopyTagsWith("nPercentile", fmt.Sprintf("%g", s.n))

		percentile := getPercentileValue(serie.Datapoints, s.n, sortedDatapointVals)
		if math.IsNaN(percentile) {
			continue
		}

		out := pointSlicePool.GetMin(len(serie.Datapoints))
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

	dataMap.Add(Req{}, output...)

	return output, nil
}

// sortedDatapointVals is an empty slice to be used for sorting datapoints.
// n must be > 0. if n > 100, the largest value is returned.
func getPercentileValue(datapoints []schema.Point, n float64, sortedDatapointVals []float64) float64 {
	sortedDatapointVals = sortedDatapointVals[:0]
	for _, p := range datapoints {
		if !math.IsNaN(p.Val) {
			sortedDatapointVals = append(sortedDatapointVals, p.Val)
		}
	}

	if len(sortedDatapointVals) == 0 {
		return math.NaN()
	}

	sort.Float64s(sortedDatapointVals)

	index := math.Min(math.Ceil(n/100.0*float64(len(sortedDatapointVals)+1)), float64(len(sortedDatapointVals))) - 1

	return sortedDatapointVals[int(index)]
}
