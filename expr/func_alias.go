package expr

import (
	"github.com/raintank/metrictank/api/models"
)

type FuncAlias struct {
	in    GraphiteFunc
	alias string
}

func NewAlias() GraphiteFunc {
	return &FuncAlias{}
}

func (s *FuncAlias) Signature() ([]Arg, []Arg) {
	return []Arg{
		ArgSeriesList{val: &s.in},
		ArgString{val: &s.alias},
	}, []Arg{ArgSeriesList{}}
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
