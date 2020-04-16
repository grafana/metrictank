package policy

import (
	"fmt"
	"math/rand"
	"strconv"
	"strings"
)

type ValuePolicy interface {
	Value(i int64) float64
}

type ValuePolicyNone struct {
}

func (v *ValuePolicyNone) Value(ts int64) float64 {
	return rand.Float64() * float64(rand.Int63n(10))
}

type ValuePolicyTimestamp struct {
}

func (v *ValuePolicyTimestamp) Value(ts int64) float64 {
	return float64(ts)
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
		return &ValuePolicyNone{}, nil
	}

	if strings.TrimSpace(p) == "timestamp" {
		return &ValuePolicyTimestamp{}, nil
	}

	split := strings.Index(p, ":")
	if split == -1 {
		return nil, fmt.Errorf("error parsing Values Policy - separator (':') not found: %s\nMake sure you don't have any spaces in your value-policy argument", p)
	}

	switch strings.TrimSpace(p[:split]) {
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
