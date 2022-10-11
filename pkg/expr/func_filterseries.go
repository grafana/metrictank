package expr

import (
	"math"

	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/consolidation"
	"github.com/grafana/metrictank/errors"
)

type FuncFilterSeries struct {
	in        GraphiteFunc
	fn        string
	operator  string
	threshold float64
}

func NewFilterSeries() GraphiteFunc {
	return &FuncFilterSeries{}
}

func NewFilterSeriesConstructor(fn string, operator string) func() GraphiteFunc {
	return func() GraphiteFunc {
		return &FuncFilterSeries{fn: fn, operator: operator}
	}
}

func (s *FuncFilterSeries) Signature() ([]Arg, []Arg) {
	if s.fn != "" && s.operator != "" {
		return []Arg{
			ArgSeriesList{val: &s.in},
			ArgFloat{key: "threshold", val: &s.threshold},
		}, []Arg{ArgSeriesList{}}
	}
	return []Arg{
		ArgSeriesList{val: &s.in},
		ArgString{key: "func", val: &s.fn, validator: []Validator{IsConsolFunc}},
		ArgString{key: "operator", val: &s.operator, validator: []Validator{IsOperator}},
		ArgFloat{key: "threshold", val: &s.threshold},
	}, []Arg{ArgSeriesList{}}

}

func (s *FuncFilterSeries) Context(context Context) Context {
	return context
}

func getOperatorFunc(operator string) (func(float64, float64) bool, error) {
	switch operator {
	case "=":
		return func(val, threshold float64) bool {
			return val == threshold
		}, nil

	case "!=":
		return func(val, threshold float64) bool {
			return val != threshold
		}, nil

	case ">":
		return func(val, threshold float64) bool {
			return val > threshold
		}, nil
	case ">=":
		return func(val, threshold float64) bool {
			return val >= threshold
		}, nil

	case "<":
		return func(val, threshold float64) bool {
			return math.IsNaN(val) || val < threshold
		}, nil
	case "<=":
		return func(val, threshold float64) bool {
			return math.IsNaN(val) || val <= threshold
		}, nil
	}
	return func(v1, v2 float64) bool { return false }, errors.NewBadRequest("Unsupported operator: " + operator)
}

func (s *FuncFilterSeries) Exec(dataMap DataMap) ([]models.Series, error) {
	series, err := s.in.Exec(dataMap)
	if err != nil {
		return nil, err
	}

	// note that s.fn has already been validated at series construction time using consolidation.IsConsolFunc
	consolidationFunc := consolidation.GetAggFunc(consolidation.FromConsolidateBy(s.fn))
	operatorFunc, err := getOperatorFunc(s.operator)
	if err != nil {
		return nil, err
	}

	out := make([]models.Series, 0, len(series))
	for _, serie := range series {
		if operatorFunc(consolidationFunc(serie.Datapoints), s.threshold) {
			out = append(out, serie)
		}
	}

	return out, nil
}
