package expr

import (
	"math"
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

type ScoredSeries struct {
	score float64
	serie models.Series
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
		if s.highest {
			return math.IsNaN(jVal) && !math.IsNaN(iVal) || iVal > jVal
		}
		return math.IsNaN(jVal) && !math.IsNaN(iVal) || iVal < jVal
	})

	if s.n > int64(len(series)) {
		s.n = int64(len(series))
	}

	for i := 0; i < int(s.n); i++ {
		series[i] = scored[i].serie
	}

	return series[:s.n], nil
}
