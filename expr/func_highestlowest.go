package expr

import (
	"math"
	"sort"

	"github.com/grafana/metrictank/consolidation"
	schema "gopkg.in/raintank/schema.v1"

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

	consolidationVals := make(map[*schema.Point]float64, len(series))

	for _, serie := range series {
		consolidationVals[&serie.Datapoints[0]] = consolidationFunc(serie.Datapoints)
	}
	seriesLess := func(i, j int) bool {
		iVal := consolidationVals[&series[i].Datapoints[0]]
		jVal := consolidationVals[&series[j].Datapoints[0]]
		if s.highest {
			return math.IsNaN(jVal) && !math.IsNaN(iVal) || iVal > jVal
		}
		return math.IsNaN(jVal) && !math.IsNaN(iVal) || iVal < jVal
	}
	sort.SliceStable(series, seriesLess)

	if s.n > int64(len(series)) {
		s.n = int64(len(series))
	}

	return series[:s.n], nil
}
