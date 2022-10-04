package expr

import (
	"github.com/grafana/metrictank/pkg/consolidation"

	"github.com/grafana/metrictank/pkg/api/models"
)

type FuncHighestLowest struct {
	in      GraphiteFunc
	n       int64
	fn      string
	highest bool
}

func NewHighestLowestConstructor(fn string, highest bool) func() GraphiteFunc {
	return func() GraphiteFunc {
		return &FuncHighestLowest{fn: fn, highest: highest}
	}
}

func (s *FuncHighestLowest) Signature() ([]Arg, []Arg) {
	if s.fn != "" {
		return []Arg{
			ArgSeriesList{val: &s.in},
			ArgInt{key: "n", val: &s.n},
		}, []Arg{ArgSeriesList{}}
	}
	return []Arg{
		ArgSeriesList{val: &s.in},
		ArgInt{key: "n", val: &s.n},
		ArgString{key: "func", val: &s.fn, validator: []Validator{IsConsolFunc}},
	}, []Arg{ArgSeriesList{}}
}

func (s *FuncHighestLowest) Context(context Context) Context {
	return context
}

func (s *FuncHighestLowest) Exec(dataMap DataMap) ([]models.Series, error) {
	series, err := s.in.Exec(dataMap)
	if err != nil {
		return nil, err
	}

	if len(series) == 0 {
		return series, nil
	}

	SortSeriesWithConsolidator(series, consolidation.FromConsolidateBy(s.fn), s.highest)

	if s.n > int64(len(series)) {
		s.n = int64(len(series))
	}

	return series[:s.n], nil
}
