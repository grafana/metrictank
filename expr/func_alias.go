package expr

import (
	"reflect"

	"github.com/raintank/metrictank/api/models"
)

type FuncAlias struct {
}

func NewAlias() Func {
	return FuncAlias{}
}

func (s FuncAlias) Signature() ([]argType, []optArg, []argType) {
	return []argType{seriesList, str}, nil, []argType{seriesList}
}

func (s FuncAlias) Init(args []*expr, namedArgs map[string]*expr) error {
	return nil
}

func (s FuncAlias) NeedRange(from, to uint32) (uint32, uint32) {
	return from, to
}

func (s FuncAlias) Exec(cache map[Req][]models.Series, named map[string]interface{}, in ...interface{}) ([]interface{}, error) {
	series, ok := in[0].([]models.Series)
	if !ok {
		return nil, ErrBadArgument{reflect.TypeOf([]models.Series{}), reflect.TypeOf(in[0])}
	}
	var out []interface{}
	for _, serie := range series {
		serie.Target = in[1].(string)
		out = append(out, serie)
	}
	return out, nil
}
