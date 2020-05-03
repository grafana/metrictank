// package metricbuilder provides various methods to build metrics, or more specifically MetricData structures
// be aware of this behavior when it comes to the MetricName property:
// | mpo | metricName has %d directive? |                           outcome |
// |-----|------------------------------|-----------------------------------|
// |   1 |                          Yes | use directive to always print 1   |
// |  >1 |                          Yes | use directive to print the number |
// |   1 |                           No |               don't use directive |
// |  >1 |                           No |               invalid: will panic |
//
package metricbuilder

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/grafana/metrictank/schema"
)

type Builder interface {
	Info() string
	// Build builds a slice of slices of MetricData's( per orgid), with time and value not set yet.
	Build(orgs, mpo, period int) [][]schema.MetricData
}

// Simple builds simple metrics by name
type Simple struct {
	MetricName string
}

func (s Simple) Info() string {
	return "metricName=" + s.MetricName
}

func (s Simple) Build(orgs, mpo, period int) [][]schema.MetricData {

	hasDirective := strings.Contains(s.MetricName, "%d")
	if mpo > 1 && !hasDirective {
		panic("MetricName directive must contain %d when using mpo >1")
	}

	out := make([][]schema.MetricData, orgs)
	for o := 0; o < orgs; o++ {
		metrics := make([]schema.MetricData, mpo)
		for m := 0; m < mpo; m++ {
			name := s.MetricName
			if hasDirective {
				name = fmt.Sprintf(name, m+1)
			}
			metrics[m] = schema.MetricData{
				Name:     name,
				OrgId:    o + 1,
				Interval: period,
				Unit:     "ms",
				Mtype:    "gauge",
			}
			metrics[m].SetId()
		}
		out[o] = metrics
	}
	return out
}

// Tagged builds metrics with a name and various tag related options
type Tagged struct {
	MetricName          string
	CustomTags          []string
	AddTags             bool
	NumUniqueCustomTags int
	NumUniqueTags       int
}

func (tb Tagged) Info() string {
	return "metricName=" + tb.MetricName
}

func (tb Tagged) Build(orgs, mpo, period int) [][]schema.MetricData {

	hasDirective := strings.Contains(tb.MetricName, "%d")
	if mpo > 1 && !hasDirective {
		panic("MetricName directive must contain %d when using mpo >1")
	}

	out := make([][]schema.MetricData, orgs)
	for o := 0; o < orgs; o++ {
		metrics := make([]schema.MetricData, mpo)
		for m := 0; m < mpo; m++ {
			var tags []string
			name := tb.MetricName
			if hasDirective {
				name = fmt.Sprintf(name, m+1)
			}

			localTags := []string{
				"secondkey=anothervalue",
				"thirdkey=onemorevalue",
				"region=west",
				"os=ubuntu",
				"anothertag=somelongervalue",
				"manymoreother=lotsoftagstointern",
				"afewmoretags=forgoodmeasure",
				"onetwothreefourfivesix=seveneightnineten",
				"lotsandlotsoftags=morefunforeveryone",
				"goodforpeoplewhojustusetags=forbasicallyeverything",
			}

			if len(tb.CustomTags) > 0 {
				if tb.NumUniqueCustomTags > 0 {
					var j int
					for j = 0; j < tb.NumUniqueCustomTags; j++ {
						tags = append(tags, tb.CustomTags[j]+strconv.Itoa(m+1))
					}
					for j < len(tb.CustomTags) {
						tags = append(tags, tb.CustomTags[j])
						j++
					}

				} else {
					tags = tb.CustomTags
				}
			}

			if tb.AddTags {
				if tb.NumUniqueTags > 0 {
					var j int
					for j = 0; j < tb.NumUniqueTags; j++ {
						tags = append(tags, localTags[j]+strconv.Itoa(m+1))
					}
					for j < len(localTags) {
						tags = append(tags, localTags[j])
						j++
					}
				} else {
					tags = localTags
				}
			}
			metrics[m] = schema.MetricData{
				Name:     name,
				OrgId:    o + 1,
				Interval: period,
				Unit:     "ms",
				Mtype:    "gauge",
				Tags:     tags,
			}
			metrics[m].SetId()
		}
		out[o] = metrics
	}
	return out
}
