package expr

import (
	"fmt"

	"github.com/raintank/metrictank/api/models"
	"github.com/raintank/metrictank/consolidation"
)

type FuncConsolidateBy struct {
	in GraphiteFunc
	by string
}

func NewConsolidateBy() GraphiteFunc {
	return &FuncConsolidateBy{}
}

func (s *FuncConsolidateBy) Signature() ([]Arg, []Arg) {
	validConsol := func(e *expr) error {
		return consolidation.Validate(e.str)
	}
	return []Arg{
		ArgSeriesList{val: &s.in},
		ArgString{val: &s.by, validator: []Validator{validConsol}},
	}, []Arg{ArgSeriesList{}}
}

func (s *FuncConsolidateBy) Context(context Context) Context {
	context.consol = consolidation.FromConsolidateBy(s.by)
	return context
}

func (s *FuncConsolidateBy) Exec(cache map[Req][]models.Series) ([]models.Series, error) {
	series, err := s.in.Exec(cache)
	if err != nil {
		return nil, err
	}
	consolidator := consolidation.FromConsolidateBy(s.by)
	var out []models.Series
	for _, series := range series {
		series.Target = fmt.Sprintf("consolidateBy(%s,\"%s\")", series.Target, s.by)
		series.Consolidator = consolidator
		series.QueryCons = consolidator
		out = append(out, series)
	}
	return out, nil
}
