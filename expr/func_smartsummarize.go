package expr

import "github.com/raintank/metrictank/api/models"

type FuncSmartSummarize struct {
	in          []models.Series
	interval    string
	fn          string
	alignToFrom bool
}

func NewSmartSummarize() Func {
	return &FuncSmartSummarize{fn: "sum"}
}

func (s *FuncSmartSummarize) Signature() ([]arg, []arg) {
	return []arg{
		argSeriesList{},
		argString{key: "interval", store: &s.interval},
		argString{key: "func", opt: true, store: &s.fn},
		argBool{key: "alignToFrom", opt: true, store: &s.alignToFrom},
	}, []arg{argSeries{}}
}

func (s *FuncSmartSummarize) NeedRange(from, to uint32) (uint32, uint32) {
	return from, to
}

func (s *FuncSmartSummarize) Exec(cache map[Req][]models.Series) ([]interface{}, error) {
	return []interface{}{s.in[0]}, nil
}
