package expr

import (
	"fmt"

	"github.com/raintank/metrictank/api/models"
	"github.com/raintank/metrictank/consolidation"
)

type FuncConsolidateBy struct {
	in Func
	by string
}

func NewConsolidateBy() Func {
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

func (s *FuncConsolidateBy) NeedRange(from, to uint32) (uint32, uint32) {
	return from, to
}

func (s *FuncConsolidateBy) Exec(cache map[Req][]models.Series) ([]models.Series, error) {
	series, err := s.in.Exec(cache)
	if err != nil {
		return nil, err
	}
	var out []models.Series
	for _, series := range series {
		series.Target = fmt.Sprintf("consolidateBy(%s,\"%s\")", series.Target, s.by)
		out = append(out, series)
	}
	return out, nil
}
