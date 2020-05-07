package policy

import (
	"errors"
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"strings"

	"github.com/raintank/dur"
)

type ValuePolicy interface {
	Value(ts int64) float64
}

type ValuePolicyRandom struct {
}

func (v *ValuePolicyRandom) Value(ts int64) float64 {
	return rand.Float64() * float64(rand.Int63n(10))
}

type ValuePolicyTimestamp struct {
}

func (v *ValuePolicyTimestamp) Value(ts int64) float64 {
	return float64(ts)
}

/*
	if you ever wanted a triangular pattern with configurable peak and stdev, that peaks at noon, this code worked:
	remainder := ts % (24 * 3600)
	num := math.Abs(float64((12 * 3600) - remainder)) // number between 0 (if ts was around noon) and 12*3600 (if ts was around midnight)
	val := rand.NormFloat64()*1000 + (v.peak * (1 - (num / float64(12*3600))))
	if val < 0 {
		val = 0
	}
	return val
*/

// ValuePolicyDailySine mimics a daily sinus-shaped trend e.g. traffic to a website
type ValuePolicyDailySine struct {
	peak   float64 // average peak values
	offset uint32  // time in seconds into the day where we should peak (0<= x <24h)
	stdev  float64 // standard deviation. typically you want 0 < x < peak

}

func NewDailySine(args string) (ValuePolicyDailySine, error) {
	argsv := strings.Split(args, ",")
	if len(argsv) != 3 {
		return ValuePolicyDailySine{}, errors.New("DailySine needs 3 comma separated options")
	}
	peak, err := strconv.ParseFloat(argsv[0], 64)
	if err != nil {
		return ValuePolicyDailySine{}, fmt.Errorf("could not parse peak value: %s", argsv[0])
	}
	offset, err := dur.ParseDuration(argsv[1])
	if err != nil {
		return ValuePolicyDailySine{}, fmt.Errorf("could not parse offset value: %s", argsv[1])
	}
	stdev, err := strconv.ParseFloat(argsv[2], 64)
	if err != nil {
		return ValuePolicyDailySine{}, fmt.Errorf("could not parse stdev value: %s", argsv[2])
	}

	return ValuePolicyDailySine{
		peak:   peak,
		offset: offset,
		stdev:  stdev,
	}, nil
}

func (v ValuePolicyDailySine) Value(ts int64) float64 {
	// a sine does a full cycle every every time we increase x by 2*Pi
	// we change this to a daily cycle
	// also, a sine normally has its peak at 1/4 into its cycle, so we adjust for that
	offset := (6 * 3600) - int64(v.offset)
	sineValue := (1 + math.Sin(2*math.Pi*float64(ts+offset)/(24*3600))) * v.peak / 2 // this value is the daily cycle, between 0 and v.peak, peaking at offset

	return math.Abs(rand.NormFloat64()*v.stdev + sineValue)

}

type ValuePolicySingle struct {
	val float64
}

func (v *ValuePolicySingle) Value(ts int64) float64 {
	return v.val
}

type ValuePolicyMultiple struct {
	lastTs int64
	idx    int
	vals   []float64
}

func (v *ValuePolicyMultiple) Value(ts int64) float64 {
	if v.lastTs == 0 {
		v.lastTs = ts
	}

	if ts == v.lastTs {
		return v.vals[v.idx]
	}

	v.lastTs = ts
	v.idx++
	if v.idx >= len(v.vals) {
		v.idx = 0
	}
	return v.vals[v.idx]
}

func ParseValuePolicy(p string) (ValuePolicy, error) {
	if p == "" {
		return &ValuePolicyRandom{}, nil
	}

	if strings.TrimSpace(p) == "timestamp" {
		return &ValuePolicyTimestamp{}, nil
	}

	split := strings.Index(p, ":")
	if split == -1 {
		return nil, fmt.Errorf("error parsing Values Policy - separator (':') not found: %s\nMake sure you don't have any spaces in your value-policy argument", p)
	}

	switch strings.TrimSpace(p[:split]) {
	case "daily-sine":
		return NewDailySine(p[split+1:])
	case "single":
		val, err := strconv.ParseFloat(strings.TrimSpace(p[split+1:]), 64)
		if err != nil {
			return nil, fmt.Errorf("could not parse value: %s", p[split+1:])
		}
		return &ValuePolicySingle{
			val: val,
		}, nil
	case "multiple":
		vals, err := parseValues(strings.TrimSpace(p[split+1:]))
		if err != nil {
			return nil, err
		}
		if len(vals) < 2 {
			return nil, fmt.Errorf("'multiple' Values Policy used, but less than 2 values were specified. Maybe you wanted to use 'single'?")
		}
		return &ValuePolicyMultiple{
			vals: vals,
		}, nil
	default:
		return nil, fmt.Errorf("error parsing Values Policy: %s", p)
	}
}

func parseValues(v string) ([]float64, error) {
	var vals []float64
	for _, s := range strings.Split(v, ",") {
		if n, err := strconv.ParseFloat(strings.TrimSpace(s), 64); err == nil {
			vals = append(vals, n)
		} else {
			return vals, fmt.Errorf("could not parse value: %s", s)
		}
	}

	return vals, nil
}
