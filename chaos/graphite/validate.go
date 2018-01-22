package graphite

import "math"

// Response is a convenience type:
// it provides original http and json decode errors, if applicable
// and also the decoded response body, if any
type Response struct {
	httpErr   error
	decodeErr error
	code      int
	traceID   string
	r         Data
}

type Validator func(resp Response) bool

// ValidateTargets returns a function that validates that the response contains exactly all named targets
func ValidateTargets(targets []string) Validator {
	return func(resp Response) bool {
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

// ValidateCorrect returns a validator with a min number,
// which will validate whether we received a "sufficiently correct"
// response.  We assume the response corresponds to a sumSeries() query
// of multiple series, typically across shards across different instances.
// the number denotes the minimum accepted value.
// e.g. with 12, all points must equal 12
// (i.e. for the use case of 12 series (1 for each shard, and each series valued at 1)
// all data from all shards is incorporated)
// to allow 4 shards being down and unaccounted for, pass 8.
// NOTE: 8 points are ignored (see comments further down) so you should only call this
// for sufficiently long series, e.g. 15 points or so.
func ValidateCorrect(num float64) Validator {
	return func(resp Response) bool {
		if resp.httpErr != nil || resp.decodeErr != nil || resp.code != 200 {
			return false
		}
		if len(resp.r) != 1 {
			return false
		}
		points := resp.r[0].Datapoints
		// first 4 points can sometimes be null (or some of them, so that sums don't add up)
		// We should at some point be more strict and clean that up,
		// but that's not in scope for these tests which focus on cluster related problems
		// last 2 points may be NaN or incomplete sums because some terms are NaN
		// this is standard graphite behavior unlike the faulty cluster behavior.
		// the faulty cluster behavior we're looking for is where terms are missing across the time range
		// (because entire shards and their series are not taken into account)
		for _, p := range points[5 : len(points)-3] {
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

// ValidaterCode returns a validator that validates whether the response has the given code
func ValidateCode(code int) Validator {
	return func(resp Response) bool {
		if resp.code == code {
			return true
		}
		return false
	}
}

// ValidatorAvgWindowed returns a validator that validates the number of series and the avg value of each series
// it is windowed to allow the dataset to include one or two values that would be evened out by a value
// just outside of the response. For example:
// response: NaN 4 4 4 5 3 4 4 4 5
// clearly here we can trust that if the avg value should be 4, that there would be a 3 coming after the response
// but we don't want to wait for that.
// NOTE: ignores up to 2 points from each series, adjust your input size accordingly for desired confidence
func ValidatorAvgWindowed(numPoints int, avg float64) Validator {
	try := func(datapoints []Point) bool {
		for i := 0; i <= 1; i++ {
		Try:
			for j := len(datapoints); j >= len(datapoints)-1; j-- {
				points := datapoints[i:j]
				sum := float64(0)
				for _, p := range points {
					if math.IsNaN(p.Val) {
						continue Try
					}
					sum += p.Val
				}
				if sum/float64(len(points)) == avg {
					return true
				}
			}
		}
		return false
	}
	return func(resp Response) bool {
		for _, series := range resp.r {
			if len(series.Datapoints) != numPoints {
				return false
			}
			if !try(series.Datapoints) {
				return false
			}
		}
		return true
	}
}
