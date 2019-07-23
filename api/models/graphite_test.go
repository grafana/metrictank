package models

import (
	"encoding/json"
	"testing"

	"github.com/grafana/metrictank/idx"
)

func TestGraphiteNames(t *testing.T) {

	cases := []struct {
		in  []idx.ArchiveInterned
		out string
	}{
		{
			in:  []idx.ArchiveInterned{},
			out: `[]`,
		},
		{
			in: []idx.ArchiveInterned{
				idx.NewArchiveInternedBare("foo"),
			},
			out: `["foo"]`,
		},
		{
			in: []idx.ArchiveInterned{
				idx.NewArchiveInternedBare("foo"),
				idx.NewArchiveInternedBare("bar"),
			},
			out: `["bar","foo"]`,
		},
		{
			in: []idx.ArchiveInterned{
				idx.NewArchiveInternedBare(`a\b`),
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
