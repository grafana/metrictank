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

func (s *FuncAliasSub) Exec(dataMap DataMap) ([]models.Series, error) {
	// support native graphite (python) groups like \3 by turning them into ${3}
	replace := groupPython.ReplaceAllString(s.replace, "$${$1}")
	series, err := s.in.Exec(dataMap)
	if err != nil {
		return nil, err
	}

	out := make([]models.Series, 0, len(series))
	for _, serie := range series {
		// TODO - graphite doesn't attempt to extract the
		// metric/expression from the series. MT probably shouldn't either.
		// This will almost certainly break some dashboards
		metric := extractMetric(serie.Target)
		if metric == "" {
			metric = serie.Target
		}
		name := s.search.ReplaceAllString(metric, replace)
		serie.Target = name
		serie.QueryPatt = name
		serie.Tags = serie.CopyTagsWith("name", name)

		out = append(out, serie)
	}
	return out, err
}
