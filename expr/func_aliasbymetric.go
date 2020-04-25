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

	out := make([]models.Series, 0, len(series))
	for _, serie := range series {
		m := strings.SplitN(extractMetric(serie.Target), ";", 2)
		mSlice := strings.Split(m[0], ".")
		base := mSlice[len(mSlice)-1]

		// append metric tags to base
		if len(m) > 1 {
			base = strings.Join([]string{base, m[1]}, ";")
		}

		serie.Target = base
		serie.QueryPatt = base

		out = append(out, serie)
	}
	return out, nil
}
