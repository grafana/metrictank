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

func (s FuncGet) Exec(dataMap DataMap) ([]models.Series, error) {
	series := dataMap[s.req]

	// this function is the only exception to the COW pattern
	// it is allowed to modify the series directly to set the needed tags
	for k := range series {
		series[k].SetTags()
	}

	return series, nil
}
