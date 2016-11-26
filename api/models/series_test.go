package models

import (
	"encoding/json"
	"gopkg.in/raintank/schema.v1"
	"testing"
)

func TestJsonMarshal(t *testing.T) {
	cases := []struct {
		in  []Series
		out string
	}{
		{
			in:  []Series{},
			out: `[]`,
		},
		{
			in: []Series{
				{
					Target:     "a",
					Datapoints: []schema.Point{},
					Interval:   60,
				},
			},
			out: `[{"target":"a","datapoints":[]}]`,
		},
		{
			in: []Series{
				{
					Target:     `a\b`,
					Datapoints: []schema.Point{},
					Interval:   60,
				},
			},
			out: `[{"target":"a\\b","datapoints":[]}]`,
		},
		{
			in: []Series{
				{
					Target: "a",
					Datapoints: []schema.Point{
						{Val: 123, Ts: 60},
						{Val: 10000, Ts: 120},
						{Val: 0, Ts: 180},
						{Val: 1, Ts: 240},
					},
					Interval: 60,
				},
			},
			out: `[{"target":"a","datapoints":[[123.000,60],[10000.000,120],[0.000,180],[1.000,240]]}]`,
		},
		{
			in: []Series{
				{
					Target: "a",
					Datapoints: []schema.Point{
						{Val: 123, Ts: 60},
						{Val: 10000, Ts: 120},
						{Val: 0, Ts: 180},
						{Val: 1, Ts: 240},
					},
					Interval: 60,
				},
				{
					Target: "foo(bar)",
					Datapoints: []schema.Point{
						{Val: 123.456, Ts: 10},
						{Val: 123.7, Ts: 20},
						{Val: 124.10, Ts: 30},
						{Val: 125.0, Ts: 40},
						{Val: 126.0, Ts: 50},
					},
					Interval: 10,
				},
			},
			out: `[{"target":"a","datapoints":[[123.000,60],[10000.000,120],[0.000,180],[1.000,240]]},{"target":"foo(bar)","datapoints":[[123.456,10],[123.700,20],[124.100,30],[125.000,40],[126.000,50]]}]`,
		},
	}

	for _, c := range cases {
		buf, err := json.Marshal(SeriesByTarget(c.in))
		if err != nil {
			t.Fatalf("failed to marshal to JSON. %s", err)
		}
		got := string(buf)
		if c.out != got {
			t.Fatalf("bad json output.\nexpected:%s\ngot:     %s\n", c.out, got)
		}
	}
}
