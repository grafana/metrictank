package expr

import (
	"sort"

	"github.com/grafana/metrictank/consolidation"

	"github.com/grafana/metrictank/api/models"
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

func (s *FuncHighestLowest) Exec(cache map[Req][]models.Series) ([]models.Series, error) {
	series, err := s.in.Exec(cache)
	if err != nil {
		return nil, err
	}

	if len(series) == 0 {
		return series, nil
	}

	consolidationFunc := consolidation.GetAggFunc(consolidation.FromConsolidateBy(s.fn))

	seriesLess := func(i, j int) bool {
		si := consolidationFunc(series[i].Datapoints)
		sj := consolidationFunc(series[j].Datapoints)
		if s.highest {
			return si > sj
		}
		return si < sj
	}
	sort.Slice(series, seriesLess)

	if s.n > int64(len(series)) {
		s.n = int64(len(series))
	}

	return series[:s.n], nil
}
