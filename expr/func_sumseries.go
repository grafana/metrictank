package expr

type FuncSumSeries struct {
}

func NewSumSeries() Func {
	return FuncSumSeries{}
}

func (s FuncSumSeries) Signature() ([]argType, []argType) {
	return []argType{seriesList}, []argType{series}
}

func (s FuncSumSeries) Init(args []*expr) error {
	return nil
}

func (s FuncSumSeries) Depends(from, to uint32) (uint32, uint32) {
	return from, to
}
