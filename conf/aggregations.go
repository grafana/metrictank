package conf

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/grafana/configparser"
)

func defaultAggregation() Aggregation {
	return Aggregation{
		Name:              "default",
		Pattern:           regexp.MustCompile(".*"),
		XFilesFactor:      0.5,
		AggregationMethod: []Method{Avg},
	}
}

// Aggregations holds the aggregation definitions
type Aggregations struct {
	Data               []Aggregation
	DefaultAggregation Aggregation
}

type Aggregation struct {
	Name              string         // mandatory (I *think* carbon allows empty name, but the docs say you should provide one)
	Pattern           *regexp.Regexp // mandatory (I *think* carbon allows empty pattern, the docs say you should provide one. but an empty string is still a pattern)
	XFilesFactor      float64        // optional. defaults to 0.5
	AggregationMethod []Method       // optional. defaults to ['average']
}

// NewAggregations create instance of Aggregations
func NewAggregations() Aggregations {
	return Aggregations{
		Data:               make([]Aggregation, 0),
		DefaultAggregation: defaultAggregation(),
	}
}

// ReadAggregations returns the defined aggregations from a storage-aggregation.conf file
// and adds the default
func ReadAggregations(file string) (Aggregations, error) {
	config, err := configparser.ReadFile(file)
	if err != nil {
		return Aggregations{}, err
	}
	_, sections, err := config.AllSections()
	if err != nil {
		return Aggregations{}, err
	}

	result := NewAggregations()

	for _, s := range sections {
		item := defaultAggregation()
		item.Name = s.Name()
		if item.Name == "" {
			return Aggregations{}, errors.New("encountered a storage-aggregation.conf section name with empty name")
		}

		if !s.Exists("pattern") {
			return Aggregations{}, fmt.Errorf("[%s]: missing pattern", item.Name)
		}

		item.Pattern, err = regexp.Compile(s.ValueOf("pattern"))
		if err != nil {
			return Aggregations{}, fmt.Errorf("[%s]: failed to parse pattern %q: %s", item.Name, s.ValueOf("pattern"), err.Error())
		}

		if s.Exists("xFilesFactor") {
			item.XFilesFactor, err = strconv.ParseFloat(s.ValueOf("xFilesFactor"), 64)
			if err != nil {
				return Aggregations{}, fmt.Errorf("[%s]: failed to parse xFilesFactor %q: %s", item.Name, s.ValueOf("xFilesFactor"), err.Error())
			}
		}

		if s.Exists("aggregationMethod") {
			aggregationMethodStr := s.ValueOf("aggregationMethod")
			methodStrs := strings.Split(aggregationMethodStr, ",")
			item.AggregationMethod = []Method{}
			for _, methodStr := range methodStrs {
				agg, err := NewMethod(methodStr)
				if err != nil {
					return result, fmt.Errorf("[%s]: %s", item.Name, err.Error())
				}
				item.AggregationMethod = append(item.AggregationMethod, agg)
			}
		}

		result.Data = append(result.Data, item)
	}

	return result, nil
}

// Match returns the correct aggregation setting for the given metric
// it can always find a valid setting, because there's a default catch all
// also returns the index of the setting, to efficiently reference it
func (a Aggregations) Match(metric string) (uint16, Aggregation) {
	for i, s := range a.Data {
		if s.Pattern.MatchString(metric) {
			return uint16(i), s
		}
	}
	return uint16(len(a.Data)), a.DefaultAggregation
}

// Get returns the aggregation setting corresponding to the given index
func (a Aggregations) Get(i uint16) Aggregation {
	if i+1 > uint16(len(a.Data)) {
		return a.DefaultAggregation
	}
	return a.Data[i]
}

func (a Aggregations) Equals(b Aggregations) bool {
	if !a.DefaultAggregation.Equals(b.DefaultAggregation) {
		return false
	}

	if len(a.Data) != len(b.Data) {
		return false
	}

	for i := range a.Data {
		if !a.Data[i].Equals(b.Data[i]) {
			return false
		}
	}
	return true
}

func (a Aggregation) Equals(b Aggregation) bool {
	if a.Name != b.Name || a.Pattern.String() != b.Pattern.String() {
		return false
	}
	diff := a.XFilesFactor - b.XFilesFactor
	if diff > 0.001 || diff < -0.001 {
		return false
	}
	if len(a.AggregationMethod) != len(b.AggregationMethod) {
		return false
	}
	for i := range a.AggregationMethod {
		if a.AggregationMethod[i] != b.AggregationMethod[i] {
			return false
		}
	}
	return true

}
