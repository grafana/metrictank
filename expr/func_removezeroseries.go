package expr

import (
	"math"

	"github.com/grafana/metrictank/api/models"
)

type FuncRemoveZeroSeries struct {
	in           GraphiteFunc
	xFilesFactor float64
}

func NewRemoveZeroSeries() GraphiteFunc {
	return &FuncRemoveZeroSeries{xFilesFactor: 0}
}

func (s *FuncRemoveZeroSeries) Signature() ([]Arg, []Arg) {

	return []Arg{
			ArgSeriesList{val: &s.in},
			ArgFloat{key: "xFilesFactor", val: &s.xFilesFactor, opt: true, validator: []Validator{WithinZeroOneInclusiveInterval}},
		}, []Arg{
			ArgSeriesList{},
		}
}

func (s *FuncRemoveZeroSeries) Context(context Context) Context {
	return context
}

func (s *FuncRemoveZeroSeries) Exec(dataMap DataMap) ([]models.Series, error) {
	series, err := s.in.Exec(dataMap)
	if err != nil {
		return nil, err
	}

	if math.IsNaN(s.xFilesFactor) {
		s.xFilesFactor = 0.0
	}

	var output []models.Series

	for _, serie := range series {
		nonNull := 0.0

		for _, p := range serie.Datapoints {
			if p.Val != 0 {
				nonNull++
			}
		}
		if nonNull != 0 && nonNull/float64(len(serie.Datapoints)) >= s.xFilesFactor {
			output = append(output, serie)
		}
	}

	return output, nil
}
