package expr

import (
	"strconv"

	"github.com/raintank/dur"
	"github.com/raintank/metrictank/api/models"
	"github.com/raintank/metrictank/consolidation"
)

type FuncSmartSummarize struct {
}

func NewSmartSummarize() GraphiteFunc {
	return FuncSmartSummarize{}
}

// smartSummarize(seriesList, intervalString, func='sum',alignToFrom=False)
func (s FuncSmartSummarize) Plan(args []*expr, namedArgs map[string]*expr, plan *Plan) (execHandler, error) {
	// Validate arguments //
	if len(args) > 4 || len(namedArgs) > 2 {
		return nil, ErrTooManyArg
	}
	if len(args) < 2 {
		return nil, ErrMissingArg
	}
	var err error

	//process intervalString
	var interval uint32
	if args[1].etype == etConst {
		intervalInt, err := strconv.Atoi(args[1].str)
		if err != nil {
			return nil, err
		}
		interval = uint32(intervalInt)
	} else {
		if args[1].etype != etString {
			return nil, ErrBadArgumentStr{"string or const", string(args[1].etype)}
		}
		interval, err = dur.ParseUsec(args[1].str)
		if err != nil {
			return nil, err
		}
	}

	// process func
	function := "avg"
	if len(args) > 2 {
		// func is a positional arg
		if args[2].etype != etString {
			return nil, ErrBadArgumentStr{"string or const", string(args[2].etype)}
		}
		function = args[2].str
		if _, ok := namedArgs["func"]; ok {
			return nil, ErrKwargSpecifiedTwice{"func"}
		}
	} else {
		// func is a named arg, or not present.
		if v, ok := namedArgs["func"]; ok {
			if v.etype != etString {
				return nil, ErrBadArgumentStr{"string or const", string(v.etype)}
			}
			function = v.str
		}
	}

	if err := consolidation.Validate(function); err != nil {
		return nil, err
	}

	// process alignToFrom
	var alignToFrom bool
	if len(args) > 3 {
		// func is a positional arg
		if args[3].etype != etBool {
			return nil, ErrBadArgumentStr{"bool", string(args[2].etype)}
		}
		alignToFrom = args[3].b
		if _, ok := namedArgs["alignToFrom"]; ok {
			return nil, ErrKwargSpecifiedTwice{"alignToFrom"}
		}
	} else {
		// func is a named arg, or not present.
		if v, ok := namedArgs["alignToFrom"]; ok {
			if v.etype != etBool {
				return nil, ErrBadArgumentStr{"bool", string(v.etype)}
			}
			alignToFrom = v.b
		}
	}

	handler, err := plan.GetHandler(args[0])
	if err != nil {
		return nil, err
	}

	return func(cache map[Req][]models.Series) ([]models.Series, error) {
		series, err := handler(cache)
		if err != nil {
			return nil, err
		}
		return s.Exec(cache, interval, function, alignToFrom, series)
	}, nil
}

func (s FuncSmartSummarize) Exec(cache map[Req][]models.Series, interval uint32, function string, alignToFrom bool, series []models.Series) ([]models.Series, error) {
	return series, nil
}
