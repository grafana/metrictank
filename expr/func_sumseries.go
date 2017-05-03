package expr

import (
	"fmt"
	"math"

	"github.com/raintank/metrictank/api/models"
	"gopkg.in/raintank/schema.v1"
)

type FuncSumSeries struct {
}

func NewSumSeries() GraphiteFunc {
	return FuncSumSeries{}
}

// sumSeries(*seriesLists)
func (s FuncSumSeries) Plan(args []*expr, namedArgs map[string]*expr, plan *Plan) (execHandler, error) {
	// Validate arguments //
	if len(args) < 1 {
		return nil, ErrMissingArg
	}
	if len(namedArgs) > 0 {
		return nil, ErrTooManyArg
	}
	handlers := make([]execHandler, len(args))
	for i, expr := range args {
		handler, err := plan.GetHandler(expr)
		if err != nil {
			return nil, err
		}
		handlers[i] = handler
	}

	return func(cache map[Req][]models.Series) ([]models.Series, error) {
		var series []models.Series
		for _, h := range handlers {
			s, err := h(cache)
			if err != nil {
				return nil, err
			}
			series = append(series, s...)
		}
		return s.Exec(cache, series)
	}, nil
}

func (s FuncSumSeries) Exec(cache map[Req][]models.Series, series []models.Series) ([]models.Series, error) {
	if len(series) == 1 {
		series[0].Target = fmt.Sprintf("sumSeries(%s)", series[0].QueryPatt)
		return series, nil
	}
	out := pointSlicePool.Get().([]schema.Point)
	for i := 0; i < len(series[0].Datapoints); i++ {
		nan := true
		point := schema.Point{
			Ts:  series[0].Datapoints[i].Ts,
			Val: 0,
		}
		for j := 0; j < len(series); j++ {
			if !math.IsNaN(series[j].Datapoints[i].Val) {
				point.Val += series[j].Datapoints[i].Val
				nan = false
			}
		}
		if nan {
			point.Val = math.NaN()
		}
		out = append(out, point)
	}
	output := models.Series{
		Target:     fmt.Sprintf("sumSeries(%s)", patternsAsArgs(series)),
		Datapoints: out,
		Interval:   series[0].Interval,
	}
	cache[Req{}] = append(cache[Req{}], output)
	return []models.Series{output}, nil
}
