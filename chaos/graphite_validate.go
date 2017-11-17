package chaos

import "math"

// response is a convenience type:
// it provides original http and json decode errors, if applicable
// and also the decoded response body, if any
type response struct {
	httpErr   error
	decodeErr error
	code      int
	traceID   string
	r         Response
}

type Validator func(resp response) bool

// validateTargets returns a function that validates that the response contains exactly all named targets
func validateTargets(targets []string) Validator {
	return func(resp response) bool {
		if resp.httpErr != nil || resp.decodeErr != nil || resp.code != 200 {
			return false
		}
		if len(resp.r) != len(targets) {
			return false
		}
		for i, r := range resp.r {
			if r.Target != targets[i] {
				return false
			}
		}
		return true
	}
}

// validateCorrect returns a validator with a min number,
// which will validate whether we received a "sufficiently correct"
// response.  We assume the response corresponds to a sumSeries() query
// of multiple series, typically across shards across different instances.
// the number denotes the minimum accepted value.
// e.g. with 12, all points must equal 12
// (i.e. for the use case of 12 series (1 for each shard, and each series valued at 1)
// all data from all shards is incorporated)
// to allow 4 shards being down and unaccounted for, pass 8.
func validateCorrect(num float64) Validator {
	return func(resp response) bool {
		if resp.httpErr != nil || resp.decodeErr != nil || resp.code != 200 {
			return false
		}
		if len(resp.r) != 1 {
			return false
		}
		points := resp.r[0].Datapoints
		// first point may be null; not sure why
		// last 2 points may be NaN or incomplete sums because some terms are NaN
		// this is standard graphite behavior unlike the faulty behavior where terms are missing across the time range
		for _, p := range points[1 : len(points)-2] {
			if math.IsNaN(p.Val) {
				return false
			}
			if p.Val > 12 || p.Val < num {
				return false
			}
		}
		return true
	}
}

// validaterCode results a validator that validates whether the response has the given code
func validateCode(code int) Validator {
	return func(resp response) bool {
		if resp.code == code {
			return true
		}
		return false
	}
}
