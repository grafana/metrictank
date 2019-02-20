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
		tags := make(map[string]string)
		for tag, value := range series[k].Tags {
			tags[tag] = value
		}
		series[k].SetTags()
		for tag, value := range tags {
			series[k].Tags[tag] = value
		}
	}

	return series, nil
}
