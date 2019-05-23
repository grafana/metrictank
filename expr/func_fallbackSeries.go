package expr

import (
	"github.com/grafana/metrictank/api/models"
)

type FuncFallbackSeries struct {
	in       GraphiteFunc
	fallback GraphiteFunc
}

func NewFallbackSeries() GraphiteFunc {
	return &FuncFallbackSeries{}
}

func (s *FuncFallbackSeries) Signature() ([]Arg, []Arg) {
	return []Arg{
		ArgSeriesList{val: &s.in},
		ArgSeriesList{val: &s.fallback, key: "fallback"},
	}, []Arg{ArgSeriesList{}}
}

func (s *FuncFallbackSeries) Context(context Context) Context {
	return context
}

func (s *FuncFallbackSeries) Exec(cache map[Req][]models.Series) ([]models.Series, error) {
	series, err := s.in.Exec(cache)
	if err != nil {
		return nil, err
	}

	if len(series) == 0 {
		series, err = s.fallback.Exec(cache)
		if err != nil {
			return nil, err
		}
	}
	return series, nil
}
