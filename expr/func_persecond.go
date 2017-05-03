package expr

import (
	"errors"
	"fmt"
	"math"

	"github.com/raintank/metrictank/api/models"
	"gopkg.in/raintank/schema.v1"
)

type FuncPerSecond struct {
}

func NewPerSecond() GraphiteFunc {
	return FuncPerSecond{}
}

// perSecond(seriesList, maxValue=None)
func (s FuncPerSecond) Plan(args []*expr, namedArgs map[string]*expr, plan *Plan) (execHandler, error) {
	// Validate arguments ///
	if len(args) > 2 || len(namedArgs) > 1 {
		return nil, ErrTooManyArg
	}
	if len(args) < 1 {
		return nil, ErrMissingArg
	}
	maxValue := math.NaN()
	lastArg := args[len(args)-1]
	if lastArg.etype != etFunc && lastArg.etype != etName {
		maxValue = lastArg.float
		if maxValue <= 0 {
			return nil, errors.New("maxValue must be integer > 0")
		}
		frac := math.Mod(maxValue, 1)
		if frac != 0 {
			return nil, errors.New("maxValue must be integer > 0")
		}
	}
	if a, ok := namedArgs["maxValue"]; ok {
		maxValue = a.float
		if maxValue <= 0 {
			return nil, errors.New("maxValue must be integer > 0")
		}
		frac := math.Mod(maxValue, 1)
		if frac != 0 {
			return nil, errors.New("maxValue must be integer > 0")
		}
	} else if len(args) == 2 {
		if args[1].etype != etConst {
			return nil, ErrBadArgumentStr{"const", string(args[1].etype)}
		}
		maxValue = args[1].float
		if maxValue <= 0 {
			return nil, errors.New("maxValue must be integer > 0")
		}
		frac := math.Mod(maxValue, 1)
		if frac != 0 {
			return nil, errors.New("maxValue must be integer > 0")
		}
	}

	handler, err := plan.GetHandler(args[0])
	if err != nil {
		return nil, err
	}

	return func(cache map[Req][]models.Series) ([]models.Series, error) {
		series, err := handler(cache)
		if err != nil {
			return nil, err
		}
		return s.Exec(cache, maxValue, series)
	}, nil
}

func (s FuncPerSecond) Exec(cache map[Req][]models.Series, maxValue float64, series []models.Series) ([]models.Series, error) {
	var outputs []models.Series
	for _, serie := range series {
		out := pointSlicePool.Get().([]schema.Point)
		for i, v := range serie.Datapoints {
			out = append(out, schema.Point{Ts: v.Ts})
			if i == 0 || math.IsNaN(v.Val) || math.IsNaN(serie.Datapoints[i-1].Val) {
				out[i].Val = math.NaN()
				continue
			}
			diff := v.Val - serie.Datapoints[i-1].Val
			if diff >= 0 {
				out[i].Val = diff / float64(serie.Interval)
			} else if !math.IsNaN(maxValue) && maxValue >= v.Val {
				out[i].Val = (maxValue + diff + 1) / float64(serie.Interval)
			} else {
				out[i].Val = math.NaN()
			}
		}
		s := models.Series{
			Target:     fmt.Sprintf("perSecond(%s)", serie.Target),
			Datapoints: out,
			Interval:   serie.Interval,
		}
		outputs = append(outputs, s)
		cache[Req{}] = append(cache[Req{}], s)
	}
	return outputs, nil
}
