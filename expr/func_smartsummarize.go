package expr

import "github.com/grafana/metrictank/api/models"

type FuncSmartSummarize struct {
	in          GraphiteFunc
	interval    string
	fn          string
	alignToFrom bool
}

func NewSmartSummarize() GraphiteFunc {
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

func (s *FuncSmartSummarize) Context(context Context) Context {
	context.MDP = 0
	context.PNGroup = 0
	context.consol = 0
	return context
}

func (s *FuncSmartSummarize) Exec(dataMap map[Req][]models.Series) ([]models.Series, error) {
	series, err := s.in.Exec(dataMap)
	return series, err
}
