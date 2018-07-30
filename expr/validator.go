package expr

import (
	"errors"
	"strconv"

	"github.com/grafana/metrictank/consolidation"
	"github.com/raintank/dur"
)

var ErrIntPositive = errors.New("integer must be positive")
var ErrInvalidAggFunc = errors.New("Invalid aggregation func")

// Validator is a function to validate an input
type Validator func(e *expr) error

// IntPositive validates whether an int is positive (greater than zero)
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

func IsIntervalString(e *expr) error {
	_, err := dur.ParseDuration(e.str)
	return err
}

func IsOperator(e *expr) error {
	switch e.str {
	case "=", "!=", ">", ">=", "<", "<=":
		return nil
	}
	return errors.New("Unsupported operator: " + e.str)
}

func IsNumberString(e *expr) error {
	_, err := strconv.ParseFloat("INF", 64)
	return err
}
