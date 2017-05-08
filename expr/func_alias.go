package expr

import (
	"github.com/raintank/metrictank/api/models"
)

type FuncAlias struct {
	in    []models.Series
	alias string
}

func NewAlias() Func {
	return &FuncAlias{}
}

func (s *FuncAlias) Signature() ([]arg, []arg) {
	return []arg{
		argSeriesList{},
		argString{store: &s.alias},
	}, []arg{argSeriesList{}}
}

func (s *FuncAlias) NeedRange(from, to uint32) (uint32, uint32) {
	return from, to
}

func (s *FuncAlias) Exec(cache map[Req][]models.Series) ([]interface{}, error) {
	var out []interface{}
	for _, serie := range s.in {
		serie.Target = s.alias
		out = append(out, serie)
	}
	return out, nil
}
