package expr

import (
	"sort"

	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/util"
)

type FuncSortByName struct {
	in      GraphiteFunc
	natural bool
	reverse bool
}

func NewSortByName() GraphiteFunc {
	return &FuncSortByName{}
}

func (s *FuncSortByName) Signature() ([]Arg, []Arg) {
	return []Arg{
			ArgSeriesList{val: &s.in},
			ArgBool{key: "natural", opt: true, val: &s.natural},
			ArgBool{key: "reverse", opt: true, val: &s.reverse},
		}, []Arg{
			ArgSeriesList{},
		}
}

func (s *FuncSortByName) Context(context Context) Context {
	return context
}

func (s *FuncSortByName) Exec(dataMap DataMap) ([]models.Series, error) {
	series, err := s.in.Exec(dataMap)
	if err != nil {
		return nil, err
	}

	// Copy series to avoid conflicting with other functions
	seriesCpy := make([]models.Series, 0, len(series))
	for _, serie := range series {
		seriesCpy = append(seriesCpy, serie)
	}

	sortFunc := seriesTargetSort{seriesCpy, stringLess}
	if s.natural {
		sortFunc.cmp = util.NaturalLess
	}

	if s.reverse {
		sort.Sort(sort.Reverse(sortFunc))
	} else {
		sort.Sort(sortFunc)
	}

	return seriesCpy, nil
}

// Provides a comparison function pointer
func stringLess(a, b string) bool {
	return a < b
}

// Pluggable comparison function, sorts by series target
type seriesTargetSort struct {
	series []models.Series
	cmp    func(string, string) bool
}

func (ss seriesTargetSort) Len() int { return len(ss.series) }

func (ss seriesTargetSort) Less(i, j int) bool {
	return ss.cmp(ss.series[i].Target, ss.series[j].Target)
}

func (ss seriesTargetSort) Swap(i, j int) { ss.series[i], ss.series[j] = ss.series[j], ss.series[i] }
