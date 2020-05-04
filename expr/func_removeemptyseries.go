package expr

import (
	"fmt"
	"math"

	"github.com/grafana/metrictank/api/models"
)

type FuncRemoveEmptySeries struct {
	in           GraphiteFunc
	xFilesFactor float64
}

func NewRemoveEmptySeries() GraphiteFunc {
	return &FuncRemoveEmptySeries{}
}

func (s *FuncRemoveEmptySeries) Signature() ([]Arg, []Arg) {

	return []Arg{
			ArgSeriesList{val: &s.in},
			ArgFloat{key: "xFilesFactor", val: &s.xFilesFactor, validator: []Validator{WithinZeroOneInclusiveInterval}},
		}, []Arg{
			ArgSeriesList{},
		}
}

func (s *FuncRemoveEmptySeries) Context(context Context) Context {
	return context
}

func (s *FuncRemoveEmptySeries) Exec(dataMap DataMap) ([]models.Series, error) {
	series, err := s.in.Exec(dataMap)
	if err != nil {
		return nil, err
	}

	if math.IsNaN(s.xFilesFactor) {
		s.xFilesFactor = 0.0
	}

	var output []models.Series
	for _, serie := range series {
		serie.Target = fmt.Sprintf("removeEmptySeries(%s, %g)", serie.Target, s.xFilesFactor)
		serie.QueryPatt = serie.Target

		if pointsXffCheck(serie.Datapoints, s.xFilesFactor) {
			output = append(output, serie)
		}
	}

	return output, nil
}
