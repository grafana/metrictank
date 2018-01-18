package expr

import (
	"regexp"

	"github.com/grafana/metrictank/api/models"
)

type FuncGrep struct {
	in             GraphiteFunc
	pattern        *regexp.Regexp
	excludeMatches bool
}

func NewGrep() GraphiteFunc {
	return &FuncGrep{excludeMatches: false}
}

func NewExclude() GraphiteFunc {
	return &FuncGrep{excludeMatches: true}
}

func (s *FuncGrep) Signature() ([]Arg, []Arg) {
	return []Arg{
			ArgSeriesList{val: &s.in},
			ArgRegex{key: "pattern", val: &s.pattern},
		}, []Arg{
			ArgSeriesList{},
		}
}

func (s *FuncGrep) Context(context Context) Context {
	return context
}

func (s *FuncGrep) Exec(cache map[Req][]models.Series) ([]models.Series, error) {
	series, err := s.in.Exec(cache)
	if err != nil {
		return nil, err
	}

	var outputs []models.Series
	for _, serie := range series {
		if s.pattern.MatchString(serie.Target) != s.excludeMatches {
			outputs = append(outputs, serie)
		}
	}
	return outputs, nil
}
