package expr

import (
	"github.com/raintank/metrictank/api/models"
)

type FuncMovingAverage struct {
	window int64
	in     []models.Series
}

func NewMovingAverage() Func {
	return &FuncMovingAverage{}
}

// note if input is 1 series, then output is too. not sure how to communicate that
func (s *FuncMovingAverage) Signature() ([]arg, []arg) {
	return []arg{
		argSeriesList{},
		// this could be an int OR a string.
		// we need to figure out the interval of the data we will consume
		// and request from -= interval * points
		// interestingly the from adjustment might mean the archive TTL is no longer sufficient and push the request into a different rollup archive, which we should probably
		// account for. let's solve all of this later.
		argInt{store: &s.window},
	}, []arg{argSeriesList{}}
}

func (s *FuncMovingAverage) NeedRange(from, to uint32) (uint32, uint32) {
	return from - uint32(s.window), to
}

func (s *FuncMovingAverage) Exec(cache map[Req][]models.Series) ([]interface{}, error) {
	//cache[Req{}] = append(cache[Req{}], out)
	return nil, nil
}
