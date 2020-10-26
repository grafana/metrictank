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

		{10, 0, 1, 10, "MSPR 10 should result in 10 if nothing else"},
		{10, 3, 1, 7, "MSPR 10 should result in 7 if 10 outstanding"},
		{10, 3, 2, 3, "MSPR 10 should result in 3 if 10 outstanding and multiplier 2 (7/2=4 would also be reasonable. we just have to pick one)"},
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
