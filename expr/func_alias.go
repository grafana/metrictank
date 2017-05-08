package expr

import (
	"github.com/raintank/metrictank/api/models"
)

type FuncAlias struct {
	in    Func
	alias string
}

func NewAlias() Func {
	return &FuncAlias{}
}

func (s *FuncAlias) Signature() ([]arg, []arg) {
	return []arg{
		argSeriesList{val: &s.in},
		argString{val: &s.alias},
	}, []arg{argSeriesList{}}
}

func (s *FuncAlias) NeedRange(from, to uint32) (uint32, uint32) {
	return from, to
}

func (s *FuncAlias) Exec(cache map[Req][]models.Series) ([]models.Series, error) {
	series, err := s.in.Exec(cache)
	if err != nil {
		return nil, err
	}
	for i := range series {
		series[i].Target = s.alias
	}
	return series, nil
}
