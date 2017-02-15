package whisper

import (
	"errors"
	"fmt"
	"strconv"
)

// parse a uint32 from a string
func parseUint32(s string) (n uint32, err error) {
	n64, err := strconv.ParseUint(s, 10, 32)	
	if err != nil {
		return
	}
	n = uint32(n64)
	return
}


// convert a value to its value in seconds, based on the unit given
func expandUnits(value uint32, unit string) (seconds uint32, err error) {
	seconds = value
	switch unit {
	case "y":
		seconds *= 52
		fallthrough
	case "w":
		seconds *= 7
		fallthrough
	case "d":
		seconds *= 24
		fallthrough
	case "h":
		seconds *= 60
		fallthrough
	case "m":
		seconds *= 60
	case "s":
		// Do nothing
	default:
		err = errors.New(fmt.Sprintf("Invalid unit: %s", unit))
	}
	return
}
