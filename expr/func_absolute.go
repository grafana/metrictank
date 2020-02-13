package expr

import (
	"fmt"
	"math"

	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/schema"
)

type FuncAbsolute struct {
	in GraphiteFunc
}

func NewAbsolute() GraphiteFunc {
	return &FuncAbsolute{}
}

func (s *FuncAbsolute) Signature() ([]Arg, []Arg) {
	return []Arg{
		ArgSeriesList{val: &s.in}}, []Arg{ArgSeriesList{}}
}

func (s *FuncAbsolute) Context(context Context) Context {
	return context
}

func (s *FuncAbsolute) Exec(dataMap DataMap) ([]models.Series, error) {
	series, err := s.in.Exec(dataMap)
	if err != nil {
		return nil, err
	}

	for i, serie := range series {
		series[i].Target = fmt.Sprintf("absolute(%s)", serie.Target)
		series[i].Tags = serie.CopyTagsWith("absolute", "1")
		series[i].QueryPatt = fmt.Sprintf("absolute(%s)", serie.QueryPatt)
		series[i].Datapoints = pointSlicePool.Get().([]schema.Point)
		for _, p := range serie.Datapoints {
			p.Val = math.Abs(p.Val)
			series[i].Datapoints = append(series[i].Datapoints, p)
		}
	}
	dataMap.Add(Req{}, series...)
	return series, nil
}
