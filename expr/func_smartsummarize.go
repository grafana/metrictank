package expr

import "github.com/raintank/metrictank/api/models"

type FuncSmartSummarize struct {
}

func NewSmartSummarize() Func {
	return FuncSmartSummarize{}
}

func (s FuncSmartSummarize) Signature() ([]argType, []optArg, []argType) {
	return []argType{seriesList, str}, []optArg{{key: "func", val: str}, {key: "alignToFrom", val: boolean}}, []argType{series}
}

func (s FuncSmartSummarize) Init(args []*expr, namedArgs map[string]*expr) error {
	return nil
}

func (s FuncSmartSummarize) NeedRange(from, to uint32) (uint32, uint32) {
	return from, to
}

func (s FuncSmartSummarize) Exec(cache map[Req][]models.Series, named map[string]interface{}, inputs ...interface{}) ([]interface{}, error) {
	return []interface{}{inputs[0]}, nil
}
