package expr

import (
	"reflect"

	"github.com/raintank/metrictank/api/models"
)

type FuncAlias struct {
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

func (s *FuncAlias) Exec(cache map[Req][]models.Series, named map[string]interface{}, in ...interface{}) ([]interface{}, error) {
	series, ok := in[0].([]models.Series)
	if !ok {
		return nil, ErrBadArgument{reflect.TypeOf([]models.Series{}), reflect.TypeOf(in[0])}
	}
	var out []interface{}
	for _, serie := range series {
		serie.Target = s.alias
		out = append(out, serie)
	}
	return out, nil
}
