package models

import (
	"encoding/json"
	"testing"

	"github.com/raintank/metrictank/idx"
)

func TestGraphiteNames(t *testing.T) {

	cases := []struct {
		in  []idx.Archive
		out string
	}{
		{
			in:  []idx.Archive{},
			out: `[]`,
		},
		{
			in: []idx.Archive{
				idx.NewArchiveBare("foo"),
			},
			out: `["foo"]`,
		},
		{
			in: []idx.Archive{
				idx.NewArchiveBare("foo"),
				idx.NewArchiveBare("bar"),
			},
			out: `["bar","foo"]`,
		},
		{
			in: []idx.Archive{
				idx.NewArchiveBare(`a\b`),
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
