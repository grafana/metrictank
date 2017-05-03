package expr

import (
	"fmt"

	"github.com/raintank/metrictank/api/models"
	"github.com/raintank/metrictank/consolidation"
)

type FuncConsolidateBy struct {
}

func NewConsolidateBy() GraphiteFunc {
	return FuncConsolidateBy{}
}

// consolidateBy(seriesList, consolidationFunc)
func (s FuncConsolidateBy) Plan(args []*expr, namedArgs map[string]*expr, plan *Plan) (execHandler, error) {
	// Validate arguments ///
	if len(args) > 2 || len(namedArgs) > 0 {
		return nil, ErrTooManyArg
	}
	if len(args) < 2 {
		return nil, ErrMissingArg
	}
	if err := consolidation.Validate(args[1].str); err != nil {
		return nil, err
	}
	consolidateBy := args[1].str
	handler, err := plan.GetHandler(args[0])
	if err != nil {
		return nil, err
	}

	return func(cache map[Req][]models.Series) ([]models.Series, error) {
		series, err := handler(cache)
		if err != nil {
			return nil, err
		}
		return s.Exec(cache, consolidateBy, series)
	}, nil

}

func (s FuncConsolidateBy) Exec(cache map[Req][]models.Series, consolidateBy string, seriesList []models.Series) ([]models.Series, error) {
	var out []models.Series
	for _, series := range seriesList {
		series.Target = fmt.Sprintf("consolidateBy(%s,\"%s\")", series.Target, consolidateBy)
		out = append(out, series)
	}
	return out, nil
}
