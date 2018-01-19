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
			ArgBool{key: "natural", val: &s.natural},
			ArgBool{key: "reverse", val: &s.reverse},
		}, []Arg{
			ArgSeriesList{},
		}
}

func (s *FuncSortByName) Context(context Context) Context {
	return context
}

func stringLess(a, b string) bool {
	return a < b
}

type seriesSort struct {
	series []models.Series
	cmp    func(string, string) bool
}

func (ss seriesSort) Len() int {
	return len(ss.series)
}

func (ss seriesSort) Less(i, j int) bool {
	return ss.cmp(ss.series[i].Target, ss.series[j].Target)
}

func (ss seriesSort) Swap(i, j int) {
	ss.series[i], ss.series[j] = ss.series[j], ss.series[i]
}

func (s *FuncSortByName) Exec(cache map[Req][]models.Series) ([]models.Series, error) {
	series, err := s.in.Exec(cache)
	if err != nil {
		return nil, err
	}

	sortFunc := seriesSort{series, stringLess}
	if s.natural {
		sortFunc.cmp = util.NaturalLess
	}

	if s.reverse {
		sort.Sort(sort.Reverse(sortFunc))
	} else {
		sort.Sort(sortFunc)
	}

	return series, nil
}
