package expr

import (
	"github.com/raintank/metrictank/api/models"
)

type FuncAlias struct {
}

func NewAlias() GraphiteFunc {
	return FuncAlias{}
}

// alias(seriesList, newName)
func (s FuncAlias) Plan(args []*expr, namedArgs map[string]*expr, plan *Plan) (execHandler, error) {
	// Validate arguments ///
	if len(args) > 2 || len(namedArgs) > 0 {
		return nil, ErrTooManyArg
	}
	if len(args) < 1 {
		return nil, ErrMissingArg
	}

	if args[1].etype != etString {
		return nil, ErrBadArgumentStr{"string", string(args[1].etype)}
	}

	alias := args[1].str

	handler, err := plan.GetHandler(args[0])
	if err != nil {
		return nil, err
	}

	return func(cache map[Req][]models.Series) ([]models.Series, error) {
		series, err := handler(cache)
		if err != nil {
			return nil, err
		}
		return s.Exec(cache, alias, series)
	}, nil
}

func (s FuncAlias) Exec(cache map[Req][]models.Series, alias string, series []models.Series) ([]models.Series, error) {
	var out []models.Series
	for _, serie := range series {
		serie.Target = alias
		out = append(out, serie)
	}
	return out, nil
}
