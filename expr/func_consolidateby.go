package expr

import (
	"fmt"
	"reflect"

	"github.com/raintank/metrictank/api/models"
	"github.com/raintank/metrictank/consolidation"
)

type FuncConsolidateBy struct {
}

func NewConsolidateBy() Func {
	return FuncConsolidateBy{}
}

func (s FuncConsolidateBy) Signature() ([]argType, []argType) {
	return []argType{seriesList, str}, []argType{seriesList}
}

func (s FuncConsolidateBy) Init(args []*expr) error {
	return consolidation.Validate(args[1].valStr)
}

func (s FuncConsolidateBy) NeedRange(from, to uint32) (uint32, uint32) {
	return from, to
}

func (s FuncConsolidateBy) Exec(cache map[Req][]models.Series, inputs ...interface{}) ([]interface{}, error) {
	var out []interface{}
	input := inputs[0]
	seriesList, ok := input.([]models.Series)
	if !ok {
		return nil, ErrBadArgument{reflect.TypeOf([]models.Series{}), reflect.TypeOf(input)}
	}
	for _, series := range seriesList {
		series.Target = fmt.Sprintf("consolidateBy(%s,\"%s\")", series.Target, inputs[1].(string))
		out = append(out, series)
	}
	return out, nil
}
