package dur

import (
	"errors"
	"strconv"
	"strings"
	"time"
)

var absoluteTimeFormats = []string{"15:04 20060102", "20060102", "01/02/06"}
var DefaultTimeZone = time.Local
var errUnknownTSpec = errors.New("parse error. unknown time format")

func ParseTSpec(s string, now time.Time, def uint32) (uint32, error) {

	if s == "" {
		return uint32(def), nil
	}

	if s == "now" {
		return uint32(now.Unix()), nil
	}

	// integer number -> unix ts
	i, err := strconv.Atoi(s)
	if err == nil {
		return uint32(i), nil
	}

	// try negative relative duration offset
	if s[0] == '-' {
		dur, err := ParseUNsec(s[1:])
		if err == nil {
			return uint32(now.Add(-time.Duration(dur) * time.Second).Unix()), nil
		}
	}

	// try positive relative duration offset
	dur, err := ParseUNsec(s)
	if err == nil {
		return uint32(now.Add(-time.Duration(dur) * time.Second).Unix()), nil
	}

	// try absolute time format

	if strings.Contains(s, "_") {
		s = strings.Replace(s, "_", " ", 1) // Go can't parse _ in date strings
	}

	for _, format := range absoluteTimeFormats {
		t, err := time.ParseInLocation(format, s, DefaultTimeZone)
		if err == nil {
			return uint32(t.Unix()), nil
		}
	}
	return 0, errUnknownTSpec
}
