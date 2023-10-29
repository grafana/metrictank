package expr

import (
	"github.com/grafana/metrictank/pkg/api/models"
)

type FuncUnique struct {
	in []GraphiteFunc
}

func NewUnique() GraphiteFunc {
	return &FuncUnique{}
}

func (s *FuncUnique) Signature() ([]Arg, []Arg) {
	return []Arg{
		ArgSeriesLists{val: &s.in}}, []Arg{ArgSeriesList{}}
}

func (s *FuncUnique) Context(context Context) Context {
	return context
}

func (s *FuncUnique) Exec(dataMap DataMap) ([]models.Series, error) {
	series, _, err := consumeFuncs(dataMap, s.in)
	if err != nil {
		return nil, err
	}
	seenNames := make(map[string]bool)
	var uniqueSeries []models.Series
	for _, serie := range series {
		if _, ok := seenNames[serie.Target]; !ok {
			seenNames[serie.Target] = true
			uniqueSeries = append(uniqueSeries, serie)
		}
	}
	return uniqueSeries, nil
}
