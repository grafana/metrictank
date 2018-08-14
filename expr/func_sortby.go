package expr

import (
	"math"
	"sort"

	"github.com/grafana/metrictank/consolidation"

	"github.com/grafana/metrictank/api/models"
)

type FuncSortBy struct {
	in      GraphiteFunc
	fn      string
	reverse bool
}

func NewSortByConstructor(fn string, reverse bool) func() GraphiteFunc {
	return func() GraphiteFunc {
		return &FuncSortBy{fn: fn, reverse: reverse}
	}
}

func (s *FuncSortBy) Signature() ([]Arg, []Arg) {
	if s.fn != "" {
		return []Arg{
			ArgSeriesList{val: &s.in},
		}, []Arg{ArgSeriesList{}}
	}
	return []Arg{
		ArgSeriesList{val: &s.in},
		ArgString{key: "func", val: &s.fn, validator: []Validator{IsConsolFunc}},
		ArgBool{key: "reverse", val: &s.reverse, opt: true},
	}, []Arg{ArgSeriesList{}}
}

func (s *FuncSortBy) Context(context Context) Context {
	return context
}

func (s *FuncSortBy) Exec(cache map[Req][]models.Series) ([]models.Series, error) {
	series, err := s.in.Exec(cache)
	if err != nil {
		return nil, err
	}

	SortSeriesWithConsolidator(series, consolidation.FromConsolidateBy(s.fn), s.reverse)

	return series, nil
}

type ScoredSeries struct {
	score float64
	serie models.Series
}

func SortSeriesWithConsolidator(series []models.Series, c consolidation.Consolidator, reverse bool) {
	consolidationFunc := consolidation.GetAggFunc(c)
	// score series by their consolidated value
	scored := make([]ScoredSeries, len(series))
	for i, serie := range series {
		scored[i] = ScoredSeries{
			score: consolidationFunc(serie.Datapoints),
			serie: serie,
		}
	}

	sort.SliceStable(scored, func(i, j int) bool {
		iVal := scored[i].score
		jVal := scored[j].score
		if reverse {
			return math.IsNaN(jVal) && !math.IsNaN(iVal) || iVal > jVal
		}
		return math.IsNaN(iVal) && !math.IsNaN(jVal) || iVal < jVal
	})

	for i := range scored {
		series[i] = scored[i].serie
	}
}
