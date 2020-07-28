package expr

import (
	"github.com/grafana/metrictank/api/models"
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
	return &FuncAggregateWithWildcards{fn: "sum"}
}

func (s *FuncAggregateWithWildcards) Signature() ([]Arg, []Arg) {
	return []Arg{
		ArgSeriesList{val: &s.in},
		ArgString{key: "func", opt: true, val: &s.fn, validator: []Validator{IsAggFunc}},
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

	type serieWithPatt struct {
		Ser  []models.Series
		Patt []string
	}

	agg := seriesAggregator{function: getCrossSeriesAggFunc(s.fn), name: s.fn}

	filtered := make(map[string]serieWithPatt)
	var keyList []string

	for _, serie := range series {
		key, err := filterNodesByPositions(serie, s.nodes)
		if err != nil {
			return nil, err
		}
		if swp, ok := filtered[key]; ok {
			swp.Ser = append(swp.Ser, serie)
			filtered[key] = swp
		} else {
			swp := serieWithPatt{
				[]models.Series{serie},
				[]string{key},
			}
			filtered[key] = swp
			keyList = append(keyList, key)
		}
	}
	out := make([]models.Series, 0, len(filtered))

	for _, key := range keyList {
		res, err := aggregate(dataMap, filtered[key].Ser, filtered[key].Patt, agg, 0)
		if err != nil {
			return nil, err
		}
		out = append(out, res...)
	}

	return out, nil
}
