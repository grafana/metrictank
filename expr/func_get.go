package expr

import "github.com/raintank/metrictank/api/models"

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

func (s FuncGet) NeedRange(from, to uint32) (uint32, uint32) {
	return from, to
}

func (s FuncGet) Exec(cache map[Req][]models.Series) ([]models.Series, error) {
	return cache[s.req], nil
}
