package expr

import (
	"fmt"

	"github.com/grafana/metrictank/internal/consolidation"
	"github.com/grafana/metrictank/pkg/api/models"
)

type FuncConsolidateBy struct {
	in GraphiteFunc
	by string
}

func NewConsolidateBy() GraphiteFunc {
	return &FuncConsolidateBy{}
}

func NewConsolidateByConstructor(by string) func() GraphiteFunc {
	return func() GraphiteFunc {
		return &FuncConsolidateBy{by: by}
	}
}

func (s *FuncConsolidateBy) Signature() ([]Arg, []Arg) {
	if s.by != "" {
		return []Arg{
			ArgSeriesList{val: &s.in},
		}, []Arg{ArgSeriesList{}}
	}
	return []Arg{
		ArgSeriesList{val: &s.in},
		ArgString{val: &s.by, validator: []Validator{IsConsolFunc}},
	}, []Arg{ArgSeriesList{}}
}

func (s *FuncConsolidateBy) Context(context Context) Context {
	context.consol = consolidation.FromConsolidateBy(s.by)
	return context
}

func (s *FuncConsolidateBy) Exec(dataMap DataMap) ([]models.Series, error) {
	series, err := s.in.Exec(dataMap)
	if err != nil {
		return nil, err
	}
	consolidator := consolidation.FromConsolidateBy(s.by)

	out := make([]models.Series, 0, len(series))
	for _, serie := range series {
		serie.Target = fmt.Sprintf("consolidateBy(%s,\"%s\")", serie.Target, s.by)
		serie.QueryPatt = fmt.Sprintf("consolidateBy(%s,\"%s\")", serie.QueryPatt, s.by)
		serie.Consolidator = consolidator
		serie.QueryCons = consolidator

		out = append(out, serie)
	}
	return out, nil
}
