package expr

import (
	"github.com/grafana/metrictank/api/models"
)

// internal function just for getting data
type FuncGet struct {
	req Req
}

func NewGet(req Req) GraphiteFunc {
	return FuncGet{req}
}

func (s FuncGet) Signature() ([]Arg, []Arg) {
	return nil, []Arg{ArgSeries{}}
}

func (s FuncGet) Context(context Context) Context {
	return context
}

func (s FuncGet) Exec(cache map[Req][]models.Series) ([]models.Series, error) {
	series := cache[s.req]

	for k := range series {
		series[k].SetTags()
	}

	return series, nil
}
