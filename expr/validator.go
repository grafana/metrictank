package expr

import "errors"

var ErrIntPositive = errors.New("integer must be positive")

type validator func(e *expr) error

func IntPositive(e *expr) error {
	if e.int < 1 {
		return ErrIntPositive
	}
	return nil
}
