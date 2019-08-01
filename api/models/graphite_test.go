package models

import (
	"encoding/json"
	"testing"

	"github.com/grafana/metrictank/interning"
)

func TestGraphiteNames(t *testing.T) {

	cases := []struct {
		in  []interning.Archive
		out string
	}{
		{
			in:  []interning.Archive{},
			out: `[]`,
		},
		{
			in: []interning.Archive{
				interning.NewArchiveBare("foo"),
			},
			out: `["foo"]`,
		},
		{
			in: []interning.Archive{
				interning.NewArchiveBare("foo"),
				interning.NewArchiveBare("bar"),
			},
			out: `["bar","foo"]`,
		},
		{
			in: []interning.Archive{
				interning.NewArchiveBare(`a\b`),
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
