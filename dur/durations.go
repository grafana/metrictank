package dur

// this package works with the following shorthands:
// Usec : unsigned (positive) number of seconds
// UNsec: like Usec, but non-zero.

import (
	"errors"
	"fmt"
	"strconv"
)

var errEmpty = errors.New("number cannot be empty")
var errNegative = errors.New("number cannot be negative")
var errNonZero = errors.New("number must be nonzero")
var errUnknownTimeUnit = errors.New("unknown time unit")

func MustParseUsec(desc, s string) uint32 {
	sec, err := ParseUsec(s)
	if err != nil {
		panic(fmt.Sprintf("%q: %s", desc, s))
	}
	return sec
}

func MustParseUNsec(desc, s string) uint32 {
	sec, err := ParseUNsec(s)
	if err != nil {
		panic(fmt.Sprintf("%q: %s", desc, s))
	}
	return sec
}

func ParseUNsec(s string) (uint32, error) {
	i, e := ParseUsec(s)
	if e == nil && i == 0 {
		return 0, errNonZero
	}
	return i, e
}

// ParseUsec converts a string expr to an Usec.
// unit defaults to s if not specified
func ParseUsec(s string) (uint32, error) {
	if s == "" {
		return 0, errEmpty
	}
	if s[0] == '-' {
		return 0, errNegative
	}
	var sum uint32
	for len(s) > 0 {
		var i int
		for i < len(s) && '0' <= s[i] && s[i] <= '9' {
			i++
		}
		var numStr string
		numStr, s = s[:i], s[i:]
		i = 0
		for i < len(s) && (s[i] < '0' || '9' < s[i]) {
			i++
		}
		var unitStr string
		unitStr, s = s[:i], s[i:]

		var units int
		switch unitStr {
		case "", "s", "sec", "secs", "second", "seconds":
			units = 1
		case "m", "min", "mins", "minute", "minutes":
			units = 60
		case "h", "hour", "hours":
			units = 60 * 60
		case "d", "day", "days":
			units = 24 * 60 * 60
		case "w", "week", "weeks":
			units = 7 * 24 * 60 * 60
		case "mon", "month", "months":
			units = 30 * 24 * 60 * 60
		case "y", "year", "years":
			units = 365 * 24 * 60 * 60
		default:
			return 0, errUnknownTimeUnit
		}

		num, err := strconv.Atoi(numStr)
		if err != nil {
			return 0, err
		}
		sum += uint32(num * units)
	}
	return sum, nil
}
