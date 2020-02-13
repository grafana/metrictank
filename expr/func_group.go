package expr

import (
	"github.com/grafana/metrictank/api/models"
)

type FuncGroup struct {
	in []GraphiteFunc
}

func NewGroup() GraphiteFunc {
	return &FuncGroup{}
}

func (s *FuncGroup) Signature() ([]Arg, []Arg) {
	return []Arg{
		ArgSeriesLists{val: &s.in}}, []Arg{ArgSeriesList{}}
}

func (s *FuncGroup) Context(context Context) Context {
	return context
}

func (s *FuncGroup) Exec(dataMap DataMap) ([]models.Series, error) {
	series, _, err := consumeFuncs(dataMap, s.in)
	if err != nil {
		return nil, err
	}

	return series, nil
}
