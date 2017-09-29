package expr

import (
	"regexp"

	"github.com/grafana/metrictank/api/models"
)

var groupPython = regexp.MustCompile(`\\(\d+)`)

type FuncAliasSub struct {
	in      GraphiteFunc
	search  *regexp.Regexp
	replace string
}

func NewAliasSub() GraphiteFunc {
	return &FuncAliasSub{}
}

func (s *FuncAliasSub) Signature() ([]Arg, []Arg) {
	return []Arg{
		ArgSeriesList{val: &s.in},
		ArgRegex{key: "search", val: &s.search},
		ArgString{key: "replace", val: &s.replace},
	}, []Arg{ArgSeries{}}
}

func (s *FuncAliasSub) Context(context Context) Context {
	return context
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
		name := s.search.ReplaceAllString(metric, replace)
		series[i].Target = name
		series[i].QueryPatt = name
	}
	return series, err
}
