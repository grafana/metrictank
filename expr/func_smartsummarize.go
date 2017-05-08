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

func (s *FuncSmartSummarize) Signature() ([]arg, []arg) {
	return []arg{
		argSeriesList{val: &s.in},
		argString{key: "interval", val: &s.interval},
		argString{key: "func", opt: true, val: &s.fn},
		argBool{key: "alignToFrom", opt: true, val: &s.alignToFrom},
	}, []arg{argSeries{}}
}

func (s *FuncSmartSummarize) NeedRange(from, to uint32) (uint32, uint32) {
	return from, to
}

func (s *FuncSmartSummarize) Exec(cache map[Req][]models.Series) ([]models.Series, error) {
	series, err := s.in.Exec(cache)
	return series, err
}
