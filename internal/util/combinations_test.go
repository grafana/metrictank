package util

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestAllCombinationsUint32(t *testing.T) {
	type testCase struct {
		in  [][]uint32
		out [][]uint32
	}
	testCases := []testCase{
		{
			in: [][]uint32{
				{1, 10},
			},
			out: [][]uint32{
				{1},
				{10},
			},
		},
		{
			in: [][]uint32{
				{1},
				{1, 10},
				{1},
			},
			out: [][]uint32{
				{1, 1, 1},
				{1, 10, 1},
			},
		},
		{
			in: [][]uint32{
				{1, 2, 3},
				{1, 5, 10},
				{4, 8},
			},
			out: [][]uint32{
				{1, 1, 4},
				{1, 1, 8},
				{1, 5, 4},
				{1, 5, 8},
				{1, 10, 4},
				{1, 10, 8},
				{2, 1, 4},
				{2, 1, 8},
				{2, 5, 4},
				{2, 5, 8},
				{2, 10, 4},
				{2, 10, 8},
				{3, 1, 4},
				{3, 1, 8},
				{3, 5, 4},
				{3, 5, 8},
				{3, 10, 4},
				{3, 10, 8},
			},
			//4, 8, 20, 40, 40, 80, 8, 16, 40, 80, 80, 160, 12, 24, 60, 120, 120, 240},
		},
	}

	for i, tc := range testCases {
		got := AllCombinationsUint32(tc.in)
		if diff := cmp.Diff(tc.out, got); diff != "" {
			t.Errorf("AllCombinationsUint32 test case %d mismatch (-want +got):\n%s", i, diff)
		}
	}
}
