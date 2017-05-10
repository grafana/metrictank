package expr

import (
	"regexp"

	"github.com/raintank/metrictank/api/models"
)

var groupPython = regexp.MustCompile(`\\(\d+)`)

type FuncAliasSub struct {
	in      Func
	search  *regexp.Regexp
	replace string
}

func NewAliasSub() Func {
	return &FuncAliasSub{}
}

func (s *FuncAliasSub) Signature() ([]arg, []arg) {
	return []arg{
		argSeriesList{val: &s.in},
		argRegex{key: "search", val: &s.search},
		argString{key: "replace", val: &s.replace},
	}, []arg{argSeries{}}
}

func (s *FuncAliasSub) NeedRange(from, to uint32) (uint32, uint32) {
	return from, to
}

func (s *FuncAliasSub) Exec(cache map[Req][]models.Series) ([]models.Series, error) {
	// support native graphite (python) groups like \3 by turning them into ${3}
	replace := groupPython.ReplaceAllString(s.replace, "$${$1}")
	series, err := s.in.Exec(cache)
	if err != nil {
		return nil, err
	}
	for i := range series {
		metric := extractMetric(series[i].Target)
		series[i].Target = s.search.ReplaceAllString(metric, replace)
	}
	return series, err
}
