package expr

import "github.com/raintank/metrictank/consolidation"

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

func (s FuncConsolidateBy) Depends(from, to uint32) (uint32, uint32) {
	return from, to
}
