package expr

import (
	"github.com/grafana/metrictank/pkg/api/models"
)

type FuncAggregateWithWildcards struct {
	in    GraphiteFunc
	fn    string
	nodes []expr
}

func NewAggregateWithWildcardsConstructor(fn string) func() GraphiteFunc {
	return func() GraphiteFunc {
		return &FuncAggregateWithWildcards{fn: fn}
	}
}

func NewAggregateWithWildcards() GraphiteFunc {
	return &FuncAggregateWithWildcards{}
}

func (s *FuncAggregateWithWildcards) Signature() ([]Arg, []Arg) {
	if s.fn == "" {
		return []Arg{
			ArgSeriesList{val: &s.in},
			ArgString{key: "func", val: &s.fn, validator: []Validator{IsAggFunc}},
			ArgStringsOrInts{key: "positions", val: &s.nodes, validator: []Validator{IntZeroOrPositive}},
		}, []Arg{ArgSeriesList{}}
	}
	return []Arg{
		ArgSeriesList{val: &s.in},
		ArgStringsOrInts{key: "positions", val: &s.nodes, validator: []Validator{IntZeroOrPositive}},
	}, []Arg{ArgSeriesList{}}
}

func (s *FuncAggregateWithWildcards) Context(context Context) Context {
	return context
}

func (s *FuncAggregateWithWildcards) Exec(dataMap DataMap) ([]models.Series, error) {
	series, err := s.in.Exec(dataMap)
	if err != nil {
		return nil, err
	}

	if len(series) == 0 {
		return series, nil
	}

	agg := seriesAggregator{function: getCrossSeriesAggFunc(s.fn), name: s.fn}

	groups := make(map[string][]models.Series)
	var keys []string

	for _, serie := range series {
		key := filterNodesByPositions(serie.Target, s.nodes)
		_, ok := groups[key]
		if !ok {
			keys = append(keys, key)
		}
		groups[key] = append(groups[key], serie)
	}
	out := make([]models.Series, 0, len(groups))

	for _, key := range keys {
		res, err := aggregate(dataMap, groups[key], []string{key}, agg, 0)
		if err != nil {
			return nil, err
		}
		res[0].Target = key
		res[0].QueryPatt = key
		out = append(out, res...)
	}

	return out, nil
}
