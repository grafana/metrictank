package expr

import (
	"math"

	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/consolidation"
)

type FuncFilterSeries struct {
	in         GraphiteFunc
	fn         string
	operator   string
	threshhold float64
}

func NewFilterSeries() GraphiteFunc {
	return &FuncFilterSeries{}
}

func (s *FuncFilterSeries) Signature() ([]Arg, []Arg) {
	return []Arg{
		ArgSeriesList{val: &s.in},
		ArgString{key: "func", val: &s.fn, validator: []Validator{IsConsolFunc}},
		ArgString{key: "operator", val: &s.operator, validator: []Validator{IsOperator}},
		ArgFloat{key: "threshhold", val: &s.threshhold},
	}, []Arg{ArgSeriesList{}}
}

func (s *FuncFilterSeries) Context(context Context) Context {
	return context
}

func (s *FuncFilterSeries) operatorFunc(val float64) bool {
	if math.IsNaN(val) {
		return true
	}
	switch s.operator {
	case "=":
		return val == s.threshhold
	case "!=":
		return val != s.threshhold
	case ">":
		return val > s.threshhold
	case ">=":
		return val >= s.threshhold
	case "<":
		return val < s.threshhold
	case "<=":
		return val <= s.threshhold
	}
	return false // should never happen
}

func (s *FuncFilterSeries) Exec(cache map[Req][]models.Series) ([]models.Series, error) {
	series, err := s.in.Exec(cache)
	if err != nil {
		return nil, err
	}

	consolidationFunc := consolidation.GetAggFunc(consolidation.FromConsolidateBy(s.fn))

	out := make([]models.Series, 0, len(series))
	for _, serie := range series {
		if s.operatorFunc(consolidationFunc(serie.Datapoints)) {
			out = append(out, serie)
		}
	}

	return out, nil
}
