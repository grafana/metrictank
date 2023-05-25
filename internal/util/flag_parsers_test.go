package util

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestParseIngestFromFlag(t *testing.T) {
	tests := []struct {
		name          string
		ingestFromStr string
		want          map[uint32]int64
		wantErr       bool
	}{
		{"empty", "", nil, false},
		{"only zeroes", "0:0", nil, true},
		{"zero timestamp", "10:0", nil, true},
		{"typical", "10:4738913", map[uint32]int64{10: 4738913}, false},
		{"missing timestamp", "0", nil, true},
		{"text no number 1", "text", nil, true},
		{"text no number 2", "1:text", nil, true},
		{"orgid overflow", "10000000000:0", nil, true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := ParseIngestFromFlags(test.ingestFromStr)
			if err == nil && test.wantErr {
				t.Errorf("ParseIngestFromFlags() should have errored but did not")
			}
			if err != nil && !test.wantErr {
				t.Errorf("ParseIngestFromFlags() errored but should not have = %v", err)
			}

			if diff := cmp.Diff(test.want, got); diff != "" {
				t.Errorf("ParseIngestFromFlags() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
