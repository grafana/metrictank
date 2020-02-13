package expr

import (
	"fmt"
	"math"

	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/schema"
)

type FuncIsNonNull struct {
	in GraphiteFunc
}

func NewIsNonNull() GraphiteFunc {
	return &FuncIsNonNull{}
}

func (s *FuncIsNonNull) Signature() ([]Arg, []Arg) {
	return []Arg{
		ArgSeriesList{val: &s.in}}, []Arg{ArgSeriesList{}}
}

func (s *FuncIsNonNull) Context(context Context) Context {
	return context
}

func (s *FuncIsNonNull) Exec(dataMap map[Req][]models.Series) ([]models.Series, error) {
	series, err := s.in.Exec(dataMap)
	if err != nil {
		return nil, err
	}

	for i, serie := range series {
		series[i].Target = fmt.Sprintf("isNonNull(%s)", serie.Target)
		series[i].QueryPatt = fmt.Sprintf("isNonNull(%s)", serie.QueryPatt)
		series[i].Tags = serie.CopyTagsWith("isNonNull", "1")
		series[i].Datapoints = pointSlicePool.Get().([]schema.Point)

		for _, p := range serie.Datapoints {
			if math.IsNaN(p.Val) {
				p.Val = 0
			} else {
				p.Val = 1
			}
			series[i].Datapoints = append(series[i].Datapoints, p)
		}
	}
	dataMap[Req{}] = append(dataMap[Req{}], series...)
	return series, nil
}
