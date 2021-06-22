package conf

import (
	"io/ioutil"
	"os"
	"regexp"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestReadAggregations(t *testing.T) {
	type testCase struct {
		title  string
		in     string
		expAgg Aggregations
		expErr bool
	}

	testCases := []testCase{
		{
			title:  "completely empty", // should result in just the default
			in:     "",
			expErr: false,
			expAgg: NewAggregations(),
		},
		{
			title:  "empty name ",
			in:     `[]`,
			expErr: true,
		},
		{
			title: "bad name format",
			in: `foo[]
			`,
			expErr: true,
		},
		{
			title: "missing pattern",
			in: `[foo]
			`,
			expErr: true,
		},
		{
			title: "invalid pattern",
			in: `[foo]
			pattern = "((("`,
			expErr: true,
		},
		{
			title: "commented out pattern is still missing",
			in: `[foo]
			;pattern = foo.*`,
			expErr: true,
		},
		{
			title: "defaults",
			in: `[foo]
			pattern = foo.*`,
			expErr: false,
			expAgg: Aggregations{
				Data: []Aggregation{
					{
						Name:              "foo",
						Pattern:           regexp.MustCompile("foo.*"),
						XFilesFactor:      0.5,
						AggregationMethod: []Method{Avg},
					},
				},
				DefaultAggregation: defaultAggregation(),
			},
		},
		{
			title: "defaults with some comments",
			in: `[foo] # comment here [does it confuse the parser if i do this?]
			pattern = foo.* # another comment here
			# pattern = this-should-be-ignored
			# and a final comment on its own line`,
			expErr: false,
			expAgg: Aggregations{
				Data: []Aggregation{
					{
						Name:              "foo",
						Pattern:           regexp.MustCompile("foo.*"),
						XFilesFactor:      0.5,
						AggregationMethod: []Method{Avg},
					},
				},
				DefaultAggregation: defaultAggregation(),
			},
		},
		{
			title: "lots of comments",
			in: `;[this is not a section]
			[foo] # [commented]
			pattern = foo.* # another comment here
			; pattern = this-should-be-ignored
			xFilesFactor = 0.8 # comment
			;xFilesFactor = 0.9
			;aggregationMethod = min,avg
			#aggregationMethod = min,avg
			aggregationMethod = max
			;aggregationMethod = min,avg
			#aggregationMethod = min,avg
			; and a final comment on its own line`,
			expErr: false,
			expAgg: Aggregations{
				Data: []Aggregation{
					{
						Name:              "foo",
						Pattern:           regexp.MustCompile("foo.*"),
						XFilesFactor:      0.8,
						AggregationMethod: []Method{Max},
					},
				},
				DefaultAggregation: defaultAggregation(),
			},
		},
		{
			title: "grafanacloud_default",
			in: `[default]
pattern = .*
xFilesFactor = 0.1
aggregationMethod = avg,sum`,
			expErr: false,
			expAgg: Aggregations{
				Data: []Aggregation{
					{
						Name:              "default",
						Pattern:           regexp.MustCompile(".*"),
						XFilesFactor:      0.1,
						AggregationMethod: []Method{Avg, Sum},
					},
				},
				DefaultAggregation: defaultAggregation(),
			},
		},
		{
			title: "graphite upstream storage-aggregation.conf example",
			in: `
# Aggregation methods for whisper files. Entries are scanned in order,
# and first match wins. This file is scanned for changes every 60 seconds
#
#  [name]
#  pattern = <regex>
#  xFilesFactor = <float between 0 and 1>
#  aggregationMethod = <average|sum|last|max|min>
#
#  name: Arbitrary unique name for the rule
#  pattern: Regex pattern to match against the metric name
#  xFilesFactor: Ratio of valid data points required for aggregation to the next retention to occur
#  aggregationMethod: function to apply to data points for aggregation
#
[min]
pattern = \.min$
xFilesFactor = 0.1
aggregationMethod = min

[max]
pattern = \.max$
xFilesFactor = 0.1
aggregationMethod = max

[sum]
pattern = \.count$
xFilesFactor = 0
# for monotonically increasing counters
aggregationMethod = max
# for counters that reset every interval (statsd-style)
#aggregationMethod = sum

[default_average]
pattern = .*
xFilesFactor = 0.5
aggregationMethod = average
			`,
			expErr: false,
			expAgg: Aggregations{
				Data: []Aggregation{
					{
						Name:              "min",
						Pattern:           regexp.MustCompile("\\.min$"),
						XFilesFactor:      0.1,
						AggregationMethod: []Method{Min},
					},
					{
						Name:              "max",
						Pattern:           regexp.MustCompile("\\.max$"),
						XFilesFactor:      0.1,
						AggregationMethod: []Method{Max},
					},
					{
						Name:              "sum",
						Pattern:           regexp.MustCompile("\\.count$"),
						XFilesFactor:      0,
						AggregationMethod: []Method{Max},
					},
					{
						Name:              "default_average",
						Pattern:           regexp.MustCompile(".*"),
						XFilesFactor:      0.5,
						AggregationMethod: []Method{Avg},
					},
				},
				DefaultAggregation: defaultAggregation(),
			},
		},
		{
			title: "graphite upstream default storage-aggregation.conf",
			in: `
		# Aggregation methods for whisper files. Entries are scanned in order,
# and first match wins. This file is scanned for changes every 60 seconds
#
#  [name]
#  pattern = <regex>
#  xFilesFactor = <float between 0 and 1>
#  aggregationMethod = <average|sum|last|max|min>
#
#  name: Arbitrary unique name for the rule
#  pattern: Regex pattern to match against the metric name
#  xFilesFactor: Ratio of valid data points required for aggregation to the next retention to occur
#  aggregationMethod: function to apply to data points for aggregation
#
[min]
pattern = \.lower$
xFilesFactor = 0.1
aggregationMethod = min

[max]
pattern = \.upper(_\d+)?$
xFilesFactor = 0.1
aggregationMethod = max

[sum]
pattern = \.sum$
xFilesFactor = 0
aggregationMethod = sum

[count]
pattern = \.count$
xFilesFactor = 0
aggregationMethod = sum

[count_legacy]
pattern = ^stats_counts.*
xFilesFactor = 0
aggregationMethod = sum

[default_average]
pattern = .*
xFilesFactor = 0.3
aggregationMethod = average
`,
			expErr: false,
			expAgg: Aggregations{
				Data: []Aggregation{
					{
						Name:              "min",
						Pattern:           regexp.MustCompile("\\.lower$"),
						XFilesFactor:      0.1,
						AggregationMethod: []Method{Min},
					},
					{
						Name:              "max",
						Pattern:           regexp.MustCompile("\\.upper(_\\d+)?$"),
						XFilesFactor:      0.1,
						AggregationMethod: []Method{Max},
					},
					{
						Name:              "sum",
						Pattern:           regexp.MustCompile("\\.sum$"),
						XFilesFactor:      0,
						AggregationMethod: []Method{Sum},
					},
					{
						Name:              "count",
						Pattern:           regexp.MustCompile("\\.count$"),
						XFilesFactor:      0,
						AggregationMethod: []Method{Sum},
					},
					{
						Name:              "count_legacy",
						Pattern:           regexp.MustCompile("^stats_counts.*"),
						XFilesFactor:      0,
						AggregationMethod: []Method{Sum},
					},
					{
						Name:              "default_average",
						Pattern:           regexp.MustCompile(".*"),
						XFilesFactor:      0.3,
						AggregationMethod: []Method{Avg},
					},
				},
				DefaultAggregation: defaultAggregation(),
			},
		},
	}

	for _, c := range testCases {
		file, err := ioutil.TempFile("", "metrictank-TestReadAggregations")
		if err != nil {
			t.Fatal(err)
		}
		defer os.Remove(file.Name())
		if _, err := file.Write([]byte(c.in)); err != nil {
			t.Fatal(err)
		}
		if err := file.Close(); err != nil {
			t.Fatal(err)
		}
		t.Logf("testing %q", c.title)
		agg, err := ReadAggregations(file.Name())
		if !c.expErr && err != nil {
			t.Fatalf("testcase %q expected no error but got error %s", c.title, err.Error())
		}
		if c.expErr && err == nil {
			t.Fatalf("testcase %q expected error but got no error", c.title)
		}
		if err == nil {
			if diff := cmp.Diff(c.expAgg, agg); diff != "" {
				t.Errorf("testcase %q mismatch (-want +got):\n%s", c.title, diff)
			}
		}
	}
}
