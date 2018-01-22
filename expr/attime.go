package expr

import (
	"fmt"
	"strconv"
	"strings"
	"unicode"
)

// Ported from graphite-web/webapp/graphite/render/attime.py
var SECONDS_STRING = "seconds"
var MINUTES_STRING = "minutes"
var HOURS_STRING = "hours"
var DAYS_STRING = "days"
var WEEKS_STRING = "weeks"
var MONTHS_STRING = "months"
var YEARS_STRING = "years"

func splitNumAlpha(s string) int {
	var start int
	if strings.HasPrefix(s, "-") || strings.HasPrefix(s, "+") {
		start++
	}
	for i, c := range s[start:] {
		if !unicode.IsNumber(c) {
			return i + start
		}
	}
	return -1
}

func getUnitString(s string) string {
	if strings.HasPrefix(s, "s") {
		return SECONDS_STRING
	}
	if strings.HasPrefix(s, "min") {
		return MINUTES_STRING
	}
	if strings.HasPrefix(s, "h") {
		return HOURS_STRING
	}
	if strings.HasPrefix(s, "d") {
		return DAYS_STRING
	}
	if strings.HasPrefix(s, "w") {
		return WEEKS_STRING
	}
	if strings.HasPrefix(s, "mon") {
		return MONTHS_STRING
	}
	if strings.HasPrefix(s, "y") {
		return YEARS_STRING
	}
	return ""
}

func ParseTimeOffset(offset string) (int, error) {
	unitIndex := splitNumAlpha(offset)
	if unitIndex < 0 {
		return 0, fmt.Errorf("Invalid offset provided %s", offset)
	}

	t, err := strconv.ParseUint(offset[:unitIndex], 10, 32)
	if err != nil {
		return 0, fmt.Errorf("Invalid offset provided: %s", err)
	}

	switch unitString := getUnitString(offset[unitIndex:]); unitString {
	case SECONDS_STRING:
		return t, nil
	case MINUTES_STRING:
		return t*60, nil
	case HOURS_STRING:
		return t*3600, nil
	case DAYS_STRING:
		return t*86400, nil
	case WEEKS_STRING:
		return t*604800, nil
	case YEARS_STRING:
		return t*31536000, nil
	}

	return 0, fmt.Errorf("Unable to parse offset %s", offset)
}
