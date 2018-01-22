package expr

import (
	"errors"

	"github.com/grafana/metrictank/consolidation"
)

var ErrIntPositive = errors.New("integer must be positive")
var ErrInvalidAggFunc = errors.New("Invalid aggregation func")

// Validator is a function to validate an input
type Validator func(e *expr) error

func IntPositive(e *expr) error {
	if e.int < 1 {
		return ErrIntPositive
	}
	return nil
}

func IsAggFunc(e *expr) error {
	if getCrossSeriesAggFunc(e.str) == nil {
		return ErrInvalidAggFunc
	}
	return nil
}

func IsConsolFunc(e *expr) error {
	return consolidation.Validate(e.str)
}

