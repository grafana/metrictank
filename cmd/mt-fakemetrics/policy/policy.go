package policy

import (
	"fmt"
	"strconv"
	"strings"
)

type Vp int

const (
	None Vp = iota
	Single
	Multiple
	Timestamp
)

type ValuePolicy struct {
	Policy Vp
	Value  float64
	Values []float64
}

func ParseValuePolicy(p string) (ValuePolicy, error) {
	if p == "" {
		return ValuePolicy{
			Policy: None,
		}, nil
	}

	if strings.TrimSpace(p) == "timestamp" {
		return ValuePolicy{
			Policy: Timestamp,
		}, nil
	}

	split := strings.Index(p, ":")
	if split == -1 {
		return ValuePolicy{}, fmt.Errorf("error parsing Values Policy - separator (':') not found: %s\nMake sure you don't have any spaces in your value-policy argument", p)
	}

	switch strings.TrimSpace(p[:split]) {
	case "single":
		val, err := strconv.ParseFloat(strings.TrimSpace(p[split+1:]), 64)
		if err != nil {
			return ValuePolicy{}, fmt.Errorf("could not parse value: %s", p[split+1:])
		}
		return ValuePolicy{
			Policy: Single,
			Value:  val,
		}, nil
	case "multiple":
		vals, err := parseValues(strings.TrimSpace(p[split+1:]))
		if err != nil {
			return ValuePolicy{}, err
		}
		if len(vals) < 2 {
			return ValuePolicy{}, fmt.Errorf("'multiple' Values Policy used, but less than 2 values were specified. Maybe you wanted to use 'single'?")
		}
		return ValuePolicy{
			Policy: Multiple,
			Values: vals,
		}, nil
	default:
		return ValuePolicy{}, fmt.Errorf("error parsing Values Policy: %s", p)
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
