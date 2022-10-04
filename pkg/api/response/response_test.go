package response

import (
	"math"

	"github.com/grafana/metrictank/pkg/api/models"
	"github.com/grafana/metrictank/pkg/schema"
)

type series struct {
	in     []models.Series
	out    string
	outCsv string
}

func testSeries() []series {
	cases := []series{
		{
			in:  []models.Series{},
			out: `[]`,
		},
		{
			in: []models.Series{
				{
					Target:     "a",
					Datapoints: []schema.Point{},
					Interval:   60,
				},
			},
			out:    `[{"target":"a","datapoints":[]}]`,
			outCsv: ``,
		},
		{
			in: []models.Series{
				{
					Target: "a",
					Datapoints: []schema.Point{
						{123, 60},
						{10000, 120},
						{0, 180},
						{1, 240},
					},
					Interval: 60,
				},
			},
			out: `[{"target":"a","datapoints":[[123,60],[10000,120],[0,180],[1,240]]}]`,
			outCsv: `a,1970-01-01 00:01:00,123
a,1970-01-01 00:02:00,10000
a,1970-01-01 00:03:00,0
a,1970-01-01 00:04:00,1
`,
		},
		{
			in: []models.Series{
				{
					Target: "a",
					Datapoints: []schema.Point{
						{123, 60},
						{10000, 120},
						{0, 180},
						{1, 240},
					},
					Interval: 60,
				},
				{
					Target: "foo(bar)",
					Datapoints: []schema.Point{
						{123.456, 10},
						{math.NaN(), 20},
						{124.10, 30},
						{125.0, 40},
						{126.0, 50},
					},
					Interval: 10,
				},
			},
			out: `[{"target":"a","datapoints":[[123,60],[10000,120],[0,180],[1,240]]},{"target":"foo(bar)","datapoints":[[123.456,10],[null,20],[124.1,30],[125,40],[126,50]]}]`,
			outCsv: `a,1970-01-01 00:01:00,123
a,1970-01-01 00:02:00,10000
a,1970-01-01 00:03:00,0
a,1970-01-01 00:04:00,1
foo(bar),1970-01-01 00:00:10,123.456
foo(bar),1970-01-01 00:00:20,
foo(bar),1970-01-01 00:00:30,124.1
foo(bar),1970-01-01 00:00:40,125
foo(bar),1970-01-01 00:00:50,126
`,
		},
	}
	return cases
}
