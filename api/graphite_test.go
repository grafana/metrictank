package api

import "testing"

func TestClusterFindLimit(t *testing.T) {
	tests := []struct {
		maxSeriesPerReq int
		outstanding     int
		multiplier      int
		exp             int
		name            string
	}{
		{0, 0, 1, 0, "MSPR 0 should always result in 0"},
		{0, 1, 1, 0, "MSPR 0 should always result in 0 regardless of outstanding"},
		{0, 1, 2, 0, "MSPR 0 should always result in 0 regardless of outstanding or multiplier"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := getClusterFindLimit(tt.maxSeriesPerReq, tt.outstanding, tt.multiplier)
			if got != tt.exp {
				t.Errorf("got %q, want %q", got, tt.exp)
			}
		})
	}

}
