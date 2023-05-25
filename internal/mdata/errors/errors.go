package errors

import (
	"errors"
)

var (
	ErrMetricTooOld               = errors.New("metric too old")
	ErrMetricNewValueForTimestamp = errors.New("new value for existing timestamp")
)
