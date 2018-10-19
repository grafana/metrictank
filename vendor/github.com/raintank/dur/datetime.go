package dur

import (
	"errors"
	"strconv"
	"strings"
	"time"
)

//var absoluteTimeFormats = []string{"15:04 20060102", "20060102", "01/02/06"}

var absoluteTimeFormats = []string{"20060102", "01/02/06", "01/02/2006"}
var errUnknownDateTimeFormat = errors.New("parse error. unknown DateTime format")
var errUnknownTimeFormat = errors.New("parse error. unknown Time format")

// MustParseDateTime parses a format string to a unix timestamp, or panics otherwise
func MustParseDateTime(s string, loc *time.Location, now time.Time, def uint32) uint32 {
	v, err := ParseDateTime(s, loc, now, def)
	if err != nil {
		panic(err)
	}
	return v
}

// ParseDateTime parses a format string to a unix timestamp, or error otherwise.
// 'loc' is the timezone to use for interpretation (when applicable)
// 'now' is a reference, in case a relative specification is given.
// 'def' is a default in case an empty specification is given.
func ParseDateTime(s string, loc *time.Location, now time.Time, def uint32) (uint32, error) {
	if s == "" {
		return uint32(def), nil
	}

	now = now.In(loc)
	s = strings.Replace(s, "-", " -", -1) // add space for splitting
	s = strings.Replace(s, "+", " +", -1) // add space for splitting
	s = strings.Replace(s, "_", " ", 1)   // this is for HH:MM_YYMMDD

	parts := strings.Fields(s)
	base := now
	next := 0

	for i, p := range parts {
		if next > i {
			continue
		}
		next = i + 1

		hour, min, sec := base.Clock()
		if i == 0 {
			hour, min, sec = 0, 0, 0
		}
	ParseSwitch:
		switch p {
		case "now":
			base = now
		case "today":
			year, month, day := now.Date()
			base = time.Date(year, month, day, hour, min, sec, 0, loc)
		case "yesterday":
			year, month, day := now.AddDate(0, 0, -1).Date()
			base = time.Date(year, month, day, hour, min, sec, 0, loc)
		case "tomorrow":
			year, month, day := now.AddDate(0, 0, 1).Date()
			base = time.Date(year, month, day, hour, min, sec, 0, loc)
		case "midnight":
			year, month, day := base.Date()
			base = time.Date(year, month, day, 0, 0, 0, 0, loc)
		case "noon":
			year, month, day := base.Date()
			base = time.Date(year, month, day, 12, 0, 0, 0, loc)
		case "teatime":
			year, month, day := base.Date()
			base = time.Date(year, month, day, 16, 0, 0, 0, loc)
		case "monday":
			year, month, day := RewindToWeekday(base, time.Monday).Date()
			base = time.Date(year, month, day, hour, min, sec, 0, loc)
		case "tuesday":
			year, month, day := RewindToWeekday(base, time.Tuesday).Date()
			base = time.Date(year, month, day, hour, min, sec, 0, loc)
		case "wednesday":
			year, month, day := RewindToWeekday(base, time.Wednesday).Date()
			base = time.Date(year, month, day, hour, min, sec, 0, loc)
		case "thursday":
			year, month, day := RewindToWeekday(base, time.Thursday).Date()
			base = time.Date(year, month, day, hour, min, sec, 0, loc)
		case "friday":
			year, month, day := RewindToWeekday(base, time.Friday).Date()
			base = time.Date(year, month, day, hour, min, sec, 0, loc)
		case "saturday":
			year, month, day := RewindToWeekday(base, time.Saturday).Date()
			base = time.Date(year, month, day, hour, min, sec, 0, loc)
		case "sunday":
			year, month, day := RewindToWeekday(base, time.Sunday).Date()
			base = time.Date(year, month, day, hour, min, sec, 0, loc)
		default:
			if p[0] == '-' {
				dur, err := ParseNDuration(p[1:])
				if err != nil {
					return 0, err
				}
				base = base.Add(-time.Duration(dur) * time.Second)
				break
			}
			if p[0] == '+' {
				dur, err := ParseNDuration(p[1:])
				if err != nil {
					return 0, err
				}
				base = base.Add(time.Duration(dur) * time.Second)
				break
			}
			// if it's a plain integer, interpret it as a unix timestamp
			// except if it's a series of numbers that looks like YYYYMMDD,
			// which will be processed further down.
			// if it's not a plain integer, we proceed trying more things
			if len(p) != 8 {
				i, err := strconv.Atoi(p)
				if err == nil {
					base = time.Unix(int64(i), 0)
					base.In(loc)
					break
				}
			}
			if IsTime(p) {
				h, m, err := ParseTime(p)
				if err != nil {
					return 0, err
				}
				year, month, day := base.Date()
				base = time.Date(year, month, day, h, m, 0, 0, loc)
				break
			}
			for _, format := range absoluteTimeFormats {
				n, err := time.ParseInLocation(format, p, loc)
				if err == nil {
					year, month, day := n.Date()
					base = time.Date(year, month, day, hour, min, sec, 0, loc)
					break ParseSwitch
				}
			}
			// see if we can parse out <monthname> <num>, or <mon-short> <num>
			if len(parts) > i+1 {
				tmp := parts[i] + " " + parts[i+1]
				n, err := time.ParseInLocation("January 2", tmp, loc)
				if err == nil {
					y, _, _ := base.Date()
					_, m, d := n.Date()
					base = time.Date(y, m, d, hour, min, sec, 0, loc)
					next = i + 2
					break
				}

				n, err = time.ParseInLocation("Jan 2", tmp, loc)
				if err == nil {
					y, _, _ := base.Date()
					_, m, d := n.Date()
					base = time.Date(y, m, d, hour, min, sec, 0, loc)
					next = i + 2
					break
				}
			}
			// we ran out of options that we recognize
			return 0, errUnknownDateTimeFormat
		}

	}
	return uint32(base.Unix()), nil
}

func IsTime(s string) bool {
	if strings.Contains(s, ":") {
		return true
	}
	if strings.HasSuffix(s, "am") || strings.HasSuffix(s, "AM") {
		return true
	}
	if strings.HasSuffix(s, "pm") || strings.HasSuffix(s, "PM") {
		return true
	}
	return false
}

// ParseTime parses a time and returns hours and minutes
func ParseTime(s string) (hour, minute int, err error) {
	var offset int
	if strings.HasSuffix(s, "am") || strings.HasSuffix(s, "AM") {
		s = s[:len(s)-2]
	}
	if strings.HasSuffix(s, "pm") || strings.HasSuffix(s, "PM") {
		offset = 12
		s = s[:len(s)-2]
	}

	parts := strings.Split(s, ":")
	if len(parts) > 2 {
		return 0, 0, errUnknownTimeFormat
	}
	if len(parts) == 2 {
		minute, err = strconv.Atoi(parts[1])
		if err != nil {
			return 0, 0, errUnknownTimeFormat
		}
	}
	hour, err = strconv.Atoi(parts[0])
	if err != nil {
		return 0, 0, errUnknownTimeFormat
	}

	return hour + offset, minute, nil
}

// RewindToWeekday moves a datetime back to the last occurence of the given weekday (potentially that day without needing to seek back)
// while retaining hour/minute/second values.
func RewindToWeekday(t time.Time, day time.Weekday) time.Time {
	for t.Weekday() != day {
		t = t.AddDate(0, 0, -1)
	}
	return t
}
