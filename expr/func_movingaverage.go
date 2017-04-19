package expr

import (
	"strconv"

	"github.com/raintank/dur"
	"github.com/raintank/metrictank/api/models"
)

type FuncMovingAverage struct {
	window uint32
}

func NewMovingAverage() Func {
	return &FuncMovingAverage{}
}

// note if input is 1 series, then output is too. not sure how to communicate that
func (s *FuncMovingAverage) Signature() ([]argType, []optArg, []argType) {
	return []argType{seriesList, str}, nil, []argType{seriesList}
}

func (s *FuncMovingAverage) Init(args []*expr, namedArgs map[string]*expr) error {
	if args[1].etype == etConst {
		points, err := strconv.Atoi(args[1].str)
		// TODO this is not correct. what really needs to happen here is figure out the interval of the data we will consume
		// and request from -= interval * points
		// interestingly the from adjustment might mean the archive TTL is no longer sufficient and push the request into a different rollup archive, which we should probably
		// account for. let's solve all of this later.
		s.window = uint32(points)
		return err
	} else {
		if args[1].etype != etString {
			panic("internal error: MovingAverage cannot parse windowSize, should already have been validated")
		}
		window, err := dur.ParseUsec(args[1].str)
		s.window = window
		return err
	}
}

func (s *FuncMovingAverage) NeedRange(from, to uint32) (uint32, uint32) {
	return from - s.window, to
}

func (s *FuncMovingAverage) Exec(cache map[Req][]models.Series, named map[string]interface{}, in ...interface{}) ([]interface{}, error) {
	//cache[Req{}] = append(cache[Req{}], out)
	return nil, nil
}
