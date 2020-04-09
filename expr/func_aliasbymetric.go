package expr

import (
	"strings"

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
		m := strings.Split(n, ";")[0]
		mSlice := strings.Split(m, ".")
		base := mSlice[len(mSlice)-1]

		series[i].Target = base
		series[i].QueryPatt = base
		series[i].Tags = series[i].CopyTagsWith("name", base)
	}
	return series, nil
}
