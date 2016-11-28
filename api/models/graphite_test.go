package models

import (
	"encoding/json"
	"testing"

	"gopkg.in/raintank/schema.v1"
)

func TestGraphiteNames(t *testing.T) {
	cases := []struct {
		in  []schema.MetricDefinition
		out string
	}{
		{
			in:  []schema.MetricDefinition{},
			out: `[]`,
		},
		{
			in: []schema.MetricDefinition{
				{
					Name: "foo",
				},
			},
			out: `["foo"]`,
		},
		{
			in: []schema.MetricDefinition{
				{
					Name: "foo",
				},
				{
					Name: "bar",
				},
			},
			out: `["bar","foo"]`,
		},
		{
			in: []schema.MetricDefinition{
				{
					Name: `a\b`,
				},
			},
			out: `["a\\b"]`,
		},
	}

	for _, c := range cases {
		buf, err := json.Marshal(MetricNames(c.in))
		if err != nil {
			t.Fatalf("failed to marshal to JSON. %s", err)
		}
		got := string(buf)
		if c.out != got {
			t.Fatalf("bad json output.\nexpected:%s\ngot:     %s\n", c.out, got)
		}
	}
}
