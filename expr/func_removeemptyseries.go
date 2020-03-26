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
			ArgFloat{key: "xFilesFactor", val: &s.xFilesFactor},
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

	var output []models.Series
	for _, serie := range series {
		if math.IsNaN(s.xFilesFactor) {
			s.xFilesFactor = 0.0
		}

		serie.Target = fmt.Sprintf("removeEmptySeries(%s, %g)", serie.Target, s.xFilesFactor)
		serie.QueryPatt = serie.Target

		notNull := 0
		for _, p := range serie.Datapoints {
			if !math.IsNaN(p.Val) {
				notNull++
			}
		}

		if xffCheck(notNull, len(serie.Datapoints), s.xFilesFactor) {
			output = append(output, serie)
		}
	}

	dataMap.Add(Req{}, series...)
	return output, nil
}

/*
xffCheck compares the ratio of notNull to total values with the xFilesFactor.
 xFilesFactor can only take values within interval [0,1]
*/
func xffCheck(notNull int, total int, xFilesFactor float64) bool {
	if notNull == 0 || total == 0 {
		return false
	}

	if xFilesFactor < 0 || xFilesFactor > 1 {
		return false
	}

	return float64(notNull)/float64(total) >= xFilesFactor
}
