package graphite

import (
	"fmt"
	"math"
	"strings"
)

// Response is a convenience type:
// it provides original http and json decode errors, if applicable
// and also the decoded response body, if any
type Response struct {
	HTTPErr   error
	DecodeErr error
	Code      int
	TraceID   string
	Decoded   Data
}

func (r Response) StringWithoutData() string {
	data := "{"
	for i, serie := range r.Decoded {
		if i > 0 {
			data += ","
		}
		data += fmt.Sprintf("%q", serie.Target)
	}
	data += "}"
	return fmt.Sprintf("<Response>{HTTPErr: %v, DecodeErr: %v, Code: %d, TraceID: %s, Decoded: %s}", r.HTTPErr, r.DecodeErr, r.Code, r.TraceID, data)
}

type Validator struct {
	Name string
	Fn   func(resp Response) bool
}

// ValidateTargets returns a function that validates that the response contains exactly all named targets
func ValidateTargets(targets []string) Validator {
	return Validator{
		Name: fmt.Sprintf("ValidateTargets(%s)", strings.Join(targets, ",")),
		Fn: func(resp Response) bool {
			if resp.HTTPErr != nil || resp.DecodeErr != nil || resp.Code != 200 {
				return false
			}
			if len(resp.Decoded) != len(targets) {
				return false
			}
			for i, r := range resp.Decoded {
				if r.Target != targets[i] {
					return false
				}
			}
			return true
		},
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
	return Validator{
		Name: fmt.Sprintf("ValidateCorrect(%f)", num),
		Fn: func(resp Response) bool {
			if resp.HTTPErr != nil || resp.DecodeErr != nil || resp.Code != 200 {
				return false
			}
			if len(resp.Decoded) != 1 {
				return false
			}
			points := resp.Decoded[0].Datapoints
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
		},
	}
}

// ValidaterCode returns a validator that validates whether the response has the given code
func ValidateCode(code int) Validator {
	return Validator{
		Name: fmt.Sprintf("ValidateCode(%d)", code),
		Fn: func(resp Response) bool {
			return resp.Code == code
		},
	}
}

// ValidatorAvgWindowed returns a validator that validates the number of series and the avg value of each series
// it is windowed to allow the dataset to include one or two values that would be evened out by a value
// just outside of the response. For example:
// response: NaN 4 4 4 5 3 4 4 4 5
// clearly here we can trust that if the avg value should be 4, that there would be a 3 coming after the response
// but we don't want to wait for that.
// NOTE: ignores up to 2 points from each series, adjust your input size accordingly for desired confidence
func ValidatorAvgWindowed(numPoints int, cmp Comparator) Validator {
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
				if cmp.Fn(sum / float64(len(points))) {
					return true
				}
			}
		}
		return false
	}
	return Validator{
		Name: fmt.Sprintf("ValidatorAvgWindowed(numPoints=%d, cmp=%s)", numPoints, cmp.Name),
		Fn: func(resp Response) bool {
			for _, series := range resp.Decoded {
				if len(series.Datapoints) != numPoints {
					return false
				}
				if !try(series.Datapoints) {
					return false
				}
			}
			return true
		},
	}
}

// ValidatorLenNulls returns a validator that validates that any of the series contained
// within the response, has a length of l and no more than prefix nulls up front.
func ValidatorLenNulls(prefix, l int) Validator {
	return Validator{
		Name: fmt.Sprintf("ValidatorLenNulls(prefix=%d, l=%d)", prefix, l),
		Fn: func(resp Response) bool {
			for _, series := range resp.Decoded {
				if len(series.Datapoints) != l {
					return false
				}
				for i, dp := range series.Datapoints {
					if math.IsNaN(dp.Val) && i+1 > prefix {
						return false
					}
				}
			}
			return true
		},
	}
}

// ValidatorAnd returns a validator that returns whether all given validators return true
// it runs them in the order given and returns as soon as one fails.
func ValidatorAnd(vals ...Validator) Validator {
	var name string
	for i, val := range vals {
		if i > 0 {
			name += " && "
			name += val.Name
		}
	}
	return Validator{
		Name: name,
		Fn: func(resp Response) bool {
			for _, val := range vals {
				if !val.Fn(resp) {
					return false
				}
			}
			return true
		},
	}
}

// ValidatorAndExhaustive returns a validator that returns whether all given validators return true
// it runs them in the order given, always runs all of them, and prints a log at each run showing the intermediate status
func ValidatorAndExhaustive(vals ...Validator) Validator {
	var name string
	for i, val := range vals {
		if i > 0 {
			name += " && "
			name += val.Name
		}
	}
	return Validator{
		Name: name,
		Fn: func(resp Response) bool {
			msg := "ValidatorAndExhaustive: "
			ret := true
			for i, val := range vals {
				ok := val.Fn(resp)
				if !ok {
					ret = false
				}
				if i > 0 {
					msg += ", "
				}
				if ok {
					msg += vals[i].Name + ":  OK"
				} else {
					msg += vals[i].Name + ":FAIL"
				}
			}
			fmt.Println(msg)
			return ret
		},
	}
}
