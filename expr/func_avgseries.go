package expr

import (
	"fmt"
	"math"

	"github.com/raintank/metrictank/api/models"
	"gopkg.in/raintank/schema.v1"
)

type FuncAvgSeries struct {
}

func NewAvgSeries() GraphiteFunc {
	return FuncAvgSeries{}
}

// averageSeries(*seriesLists)
func (s FuncAvgSeries) Plan(args []*expr, namedArgs map[string]*expr, plan *Plan) (execHandler, error) {
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

func (s FuncAvgSeries) Exec(cache map[Req][]models.Series, series []models.Series) ([]models.Series, error) {
	out := pointSlicePool.Get().([]schema.Point)
	for i := 0; i < len(series[0].Datapoints); i++ {
		num := 0
		sum := float64(0)
		for j := 0; j < len(series); j++ {
			p := series[j].Datapoints[i].Val
			if !math.IsNaN(p) {
				num++
				sum += p
			}
		}
		point := schema.Point{
			Ts: series[0].Datapoints[i].Ts,
		}
		if num == 0 {
			point.Val = math.NaN()
		} else {
			point.Val = sum / float64(num)
		}
		out = append(out, point)
	}
	output := models.Series{
		Target:     fmt.Sprintf("averageSeries(%s)", patternsAsArgs(series)),
		Datapoints: out,
		Interval:   series[0].Interval,
	}
	cache[Req{}] = append(cache[Req{}], output)

	return []models.Series{output}, nil
}
