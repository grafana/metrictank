package dur

import (
	"fmt"
	"testing"
	"time"
)

const outFormat = "15:04:05 2006-Jan-02"

func TestParseDateTime(t *testing.T) {

	// Let's use CET (UTC+1) as our TZ for the tests. It shows that custom TZ support works,
	// while being minimally different from UTC in case you want to follow the logic/math.
	// IOW things like "midnight" will be in this TZ and the nows that are used are also
	// interpreted in this TZ (the same "now" can be a different day in different timezones)
	loc, err := time.LoadLocation("CET")
	if err != nil {
		panic(err)
	}

	// a few values for "now"
	jul8 := time.Date(2017, time.July, 8, 15, 30, 0, 0, loc)   // saturday 8 Jul 2017 15:30 CET
	jul9 := time.Date(2017, time.July, 9, 15, 30, 0, 0, loc)   // sunday   9 Jul 2017 15:30 CET
	jul10 := time.Date(2017, time.July, 10, 15, 30, 0, 0, loc) // monday  10 Jul 2017 15:30 CET
	jul11 := time.Date(2017, time.July, 11, 15, 30, 0, 0, loc) // tuesday 11 Jul 2017 15:30 CET

	var tests = []struct {
		in       string
		now      time.Time
		expTsStr string
		expErr   error
	}{
		{"now", jul8, "15:30:00 2017-Jul-08", nil},
		{"-1d", jul8, "15:30:00 2017-Jul-07", nil},
		{"-7d", jul8, "15:30:00 2017-Jul-01", nil},
		{"-10d", jul8, "15:30:00 2017-Jun-28", nil},
		{"-10d5h", jul8, "10:30:00 2017-Jun-28", nil},
		{"now-1d", jul8, "15:30:00 2017-Jul-07", nil},
		{"now-7d", jul8, "15:30:00 2017-Jul-01", nil},
		{"now-10d", jul8, "15:30:00 2017-Jun-28", nil},
		{"now-10d5h", jul8, "10:30:00 2017-Jun-28", nil},

		// YYYYMMDD
		{"20091201", jul8, "00:00:00 2009-Dec-01", nil}, // graphite assumes 00:00:00
		{"20091231", jul8, "00:00:00 2009-Dec-31", nil}, // graphite assumes 00:00:00

		// HH:MM_YYMMDD
		{"04:00_20110501", jul8, "04:00:00 2011-May-01", nil},
		{"16:00_20110501", jul8, "16:00:00 2011-May-01", nil},

		// HH:MM YYMMDD
		{"4:00 20110501", jul8, "04:00:00 2011-May-01", nil},
		{"04:00 20110501", jul8, "04:00:00 2011-May-01", nil},
		{"16:00 20110501", jul8, "16:00:00 2011-May-01", nil},

		// a unix timestamp which cannot be confused with YYYYMMDD format above
		// one of my favorites. valentines day 2009 in central europe.
		{"1234567890", jul8, "00:31:30 2009-Feb-14", nil},

		// MM/DD/YY
		{"01/02/2000", jul8, "00:00:00 2000-Jan-02", nil},
		{"05/01/2000", jul8, "00:00:00 2000-May-01", nil},

		// other at(1)-compatible time format.
		{"midnight", jul8, "00:00:00 2017-Jul-08", nil},
		{"noon", jul8, "12:00:00 2017-Jul-08", nil},
		{"teatime", jul8, "16:00:00 2017-Jul-08", nil},

		{"yesterday", jul8, "00:00:00 2017-Jul-07", nil}, // graphite assumes 00:00:00
		{"today", jul8, "00:00:00 2017-Jul-08", nil},     // graphite assumes 00:00:00
		{"tomorrow", jul8, "00:00:00 2017-Jul-09", nil},  // graphite assumes 00:00:00 I think, but hard to tell, because graphite doesn't return future data

		{"noon yesterday", jul8, "12:00:00 2017-Jul-07", nil},
		{"3am tomorrow", jul8, "03:00:00 2017-Jul-09", nil},
		{"3AM tomorrow", jul8, "03:00:00 2017-Jul-09", nil},
		{"6pm today", jul8, "18:00:00 2017-Jul-08", nil},
		{"6PM today", jul8, "18:00:00 2017-Jul-08", nil},
		{"6:00PM today", jul8, "18:00:00 2017-Jul-08", nil},
		{"06:00PM today", jul8, "18:00:00 2017-Jul-08", nil},
		{"06PM today", jul8, "18:00:00 2017-Jul-08", nil},
		{"january 1", jul8, "00:00:00 2017-Jan-01", nil},
		{"jan 1", jul8, "00:00:00 2017-Jan-01", nil},
		{"march 8", jul8, "00:00:00 2017-Mar-08", nil},
		{"mar 8", jul8, "00:00:00 2017-Mar-08", nil},
		{"monday", jul8, "00:00:00 2017-Jul-03", nil},
		{"monday", jul9, "00:00:00 2017-Jul-03", nil},
		{"monday", jul10, "00:00:00 2017-Jul-10", nil},
		{"monday", jul11, "00:00:00 2017-Jul-10", nil},

		{"noon 08/12/98", jul8, "12:00:00 1998-Aug-12", nil},
		{"noon 08/12/2002", jul8, "12:00:00 2002-Aug-12", nil},
		{"midnight 20170812", jul8, "00:00:00 2017-Aug-12", nil},
		{"noon tomorrow", jul8, "12:00:00 2017-Jul-09", nil},
	}

	for i, tt := range tests {
		ts, err := ParseDateTime(tt.in, loc, tt.now, 0)
		if tt.expErr == nil && err != nil {
			t.Errorf("case %d: ParseDateTime(%q, %d, 0) expected err nil, got err %v", i, tt.in, tt.now, err)
		}
		if tt.expErr != nil && err == nil {
			t.Errorf("case %d: ParseDateTime(%q, %d, 0) expected err %v, got err nil", i, tt.in, tt.now, tt.expErr)
		}

		expTime, err := time.ParseInLocation(outFormat, tt.expTsStr, loc)
		if err != nil {
			panic(fmt.Sprintf("case %d: error parsing expTime out of input %q with format str %q: %v", i, tt.expTsStr, outFormat, err))
		}
		expTs := uint32(expTime.Unix())

		if ts != expTs {
			t.Errorf("case %d: ParseDateTime(%q, %d, 0) expected %d (%q) got %d (%q)", i, tt.in, tt.now, expTs, tt.expTsStr, ts, time.Unix(int64(ts), 0).In(loc).Format(outFormat))
		}
	}
}

var res uint32

func BenchmarkParseDateTime(b *testing.B) {
	patts := []string{"now", "-7d", "now-7d10h3min", "today", "monday", "march 10", "04:00 20110501", "4pm 20110501", "1234567890", "teatime 12/25/1998"}
	loc, err := time.LoadLocation("CET")
	if err != nil {
		panic(err)
	}
	now := time.Date(2017, time.July, 8, 15, 30, 0, 0, loc) // saturday 8 Jul 2017 15:30 CET
	for _, patt := range patts {
		b.Run(patt, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				res, err = ParseDateTime(patt, loc, now, 0)
			}
			if err != nil {
				panic(err)
			}
		})
	}
}
