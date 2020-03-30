package expr

import (
	"github.com/grafana/metrictank/api/models"
)

type FuncAliasByMetric struct {
	in GraphiteFunc
}

func NewAliasByMetric() GraphiteFunc {
	return &FuncAliasByMetric{}
}

func (s *FuncAliasByMetric) Signature() ([]Arg, []Arg) {
	return []Arg{
		ArgSeriesList{val: &s.in},
	}, []Arg{ArgSeries{}}
}

func (s *FuncAliasByMetric) Context(context Context) Context {
	return context
}

func (s *FuncAliasByMetric) Exec(dataMap DataMap) ([]models.Series, error) {
	series, err := s.in.Exec(dataMap)
	if err != nil {
		return nil, err
	}
	for i, serie := range series {
		n := extractMetric(serie.Target)
		series[i].Target = n
		series[i].QueryPatt = n
	}
	return series, nil
}
