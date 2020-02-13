package expr

import (
	"github.com/grafana/metrictank/api/models"
)

type FuncAliasByNode struct {
	in    GraphiteFunc
	nodes []expr
}

func NewAliasByNode() GraphiteFunc {
	return &FuncAliasByNode{}
}

func (s *FuncAliasByNode) Signature() ([]Arg, []Arg) {
	return []Arg{
		ArgSeriesList{val: &s.in},
		ArgStringsOrInts{val: &s.nodes},
	}, []Arg{ArgSeries{}}
}

func (s *FuncAliasByNode) Context(context Context) Context {
	return context
}

func (s *FuncAliasByNode) Exec(dataMap DataMap) ([]models.Series, error) {
	series, err := s.in.Exec(dataMap)
	if err != nil {
		return nil, err
	}
	for i, serie := range series {
		n := aggKey(serie, s.nodes)
		series[i].Target = n
		series[i].QueryPatt = n
		series[i].Tags = series[i].CopyTagsWith("name", n)
	}
	return series, nil
}
