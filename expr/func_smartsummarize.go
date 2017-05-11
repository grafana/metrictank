package expr

import "github.com/raintank/metrictank/api/models"

type FuncSmartSummarize struct {
	in          Func
	interval    string
	fn          string
	alignToFrom bool
}

func NewSmartSummarize() Func {
	return &FuncSmartSummarize{fn: "sum"}
}

func (s *FuncSmartSummarize) Signature() ([]Arg, []Arg) {
	return []Arg{
		ArgSeriesList{val: &s.in},
		ArgString{key: "interval", val: &s.interval},
		ArgString{key: "func", opt: true, val: &s.fn},
		ArgBool{key: "alignToFrom", opt: true, val: &s.alignToFrom},
	}, []Arg{ArgSeries{}}
}

func (s *FuncSmartSummarize) NeedRange(from, to uint32) (uint32, uint32) {
	return from, to
}

func (s *FuncSmartSummarize) Exec(cache map[Req][]models.Series) ([]models.Series, error) {
	series, err := s.in.Exec(cache)
	return series, err
}
