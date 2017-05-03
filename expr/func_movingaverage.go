package expr

import (
	"strconv"

	"github.com/raintank/dur"
	"github.com/raintank/metrictank/api/models"
)

type FuncMovingAverage struct {
}

func NewMovingAverage() GraphiteFunc {
	return FuncMovingAverage{}
}

//movingAverage(seriesList, windowSize)
func (s FuncMovingAverage) Plan(args []*expr, namedArgs map[string]*expr, plan *Plan) (execHandler, error) {
	// Validate arguments //
	if len(args) > 2 || len(namedArgs) > 0 {
		return nil, ErrTooManyArg
	}
	if len(args) < 2 {
		return nil, ErrMissingArg
	}
	var window uint32
	var err error
	if args[1].etype == etConst {
		points, err := strconv.Atoi(args[1].str)
		if err != nil {
			return nil, err
		}
		// TODO this is not correct. what really needs to happen here is figure out the interval of the data we will consume
		// and request from -= interval * points
		// interestingly the from adjustment might mean the archive TTL is no longer sufficient and push the request into a different rollup archive, which we should probably
		// account for. let's solve all of this later.
		window = uint32(points)
	} else {
		if args[1].etype != etString {
			return nil, ErrBadArgumentStr{"string or const", string(args[1].etype)}
		}
		window, err = dur.ParseUsec(args[1].str)
		if err != nil {
			return nil, err
		}
	}

	// adjust the from time
	plan.From = plan.From - window

	handler, err := plan.GetHandler(args[0])
	if err != nil {
		return nil, err
	}

	return func(cache map[Req][]models.Series) ([]models.Series, error) {
		series, err := handler(cache)
		if err != nil {
			return nil, err
		}
		return s.Exec(cache, window, series)
	}, nil
}

func (s FuncMovingAverage) Exec(cache map[Req][]models.Series, window uint32, series []models.Series) ([]models.Series, error) {
	//cache[Req{}] = append(cache[Req{}], out)
	return series, nil
}
