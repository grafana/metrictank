package expr

import (
	"github.com/grafana/metrictank/pkg/api/models"
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

	out := make([]models.Series, 0, len(series))
	for _, serie := range series {
		n := aggKey(serie, s.nodes)
		serie.Target = n
		serie.QueryPatt = n
		serie.Tags = serie.CopyTagsWith("name", n)
		out = append(out, serie)
	}
	return out, nil
}
