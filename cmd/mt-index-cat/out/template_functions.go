package out

import (
	"log"
	"math/rand"
	"strings"
)

// random choice between replacing a node with a wildcard, a char with a wildcard, and passthrough
func pattern(in string) string {
	mode := rand.Intn(3)
	if mode == 0 {
		// in this mode, replaces a node with a wildcard
		parts := strings.Split(in, ".")
		parts[rand.Intn(len(parts))] = "*"
		return strings.Join(parts, ".")
	} else if mode == 1 {
		// randomly replace chars with a *
		// note that in 1/5 cases, nothing happens
		// and otherwise, sometimes valid patterns are produced,
		// but it's also possible to produce patterns that won't match anything (if '.' was taken out)
		if len(in) < 5 {
			log.Fatalf("metric %q too short for pattern replacement", in)
		}
		chars := rand.Intn(5)
		pos := rand.Intn(len(in) - chars)
		return in[0:pos] + "*" + in[pos+chars:]
	}
	// mode 3: do nothing :)
	return in
}

// age returns age compared to time of query
func age(in int64) int64 {
	return QueryTime - in
}

// roundDuration rounds a second-specified duration for rough classification
func roundDuration(in int64) int64 {
	if in <= 10 { // 10s -> don't round
		return in
	} else if in <= 60 { // 1min -> round to 10s
		return round(in, 10)
	} else if in <= 600 { // 10min -> round to 1min
		return round(in, 60)
	} else if in <= 3600 { // 1h -> round to 10min
		return round(in, 600)
	} else if in <= 3600*24 { // 24h -> round to 1h
		return round(in, 3600)
	} else if in <= 3600*24*7 { // 7d -> round to 1d
		return round(in, 3600*24)
	} else if in <= 3600*24*30 { // 30d -> round to 7d
		return round(in, 3600*24*7)
	}
	// default to rounding to months
	return round(in, 3600*24*30)
}

// round rounds number d to the nearest r-boundary
func round(d, r int64) int64 {
	neg := d < 0
	if neg {
		d = -d
	}
	if m := d % r; m+m < r {
		d = d - m
	} else {
		d = d + r - m
	}
	if neg {
		return -d
	}
	return d
}
