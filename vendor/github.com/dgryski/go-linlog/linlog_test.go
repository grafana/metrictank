package linlog

import (
	"reflect"
	"testing"
)

func TestBinOf(t *testing.T) {

	tests := []struct {
		sz  uint64
		l   uint64
		b   uint64
		wb  uint64
		wsz uint64
	}{
		{
			0, 4, 2,
			0, 0,
		},
		{
			1, 4, 2,
			1, 4,
		},
		{
			4, 4, 2,
			1, 4,
		},
		{
			5, 4, 2,
			2, 8,
		},
		{
			9, 4, 2,
			3, 12,
		},
		{
			15, 4, 2,
			4, 16,
		},
		{
			17, 4, 2,
			5, 20,
		},
		{
			34, 4, 2,
			9, 40,
		},
	}

	for _, tt := range tests {
		if r, b := BinOf(tt.sz, tt.l, tt.b); r != tt.wsz || b != tt.wb {
			t.Errorf("BinOf(%d,%d,%d)=(%d,%d), want (%d,%d)", tt.sz, tt.l, tt.b, r, b, tt.wsz, tt.wb)
		}
	}
}

func TestBinDownOf(t *testing.T) {

	tests := []struct {
		sz  uint64
		l   uint64
		b   uint64
		wb  uint64
		wsz uint64
	}{
		{
			0, 4, 2,
			0, 0,
		},
		{
			1, 4, 2,
			0, 0,
		},
		{
			3, 4, 2,
			0, 0,
		},
		{
			4, 4, 2,
			1, 4,
		},
		{
			7, 4, 2,
			1, 4,
		},
		{
			15, 4, 2,
			3, 12,
		},
		{
			16, 4, 2,
			4, 16,
		},
		{
			17, 4, 2,
			4, 16,
		},
		{
			34, 4, 2,
			8, 32,
		},
	}

	for _, tt := range tests {
		if r, b := BinDownOf(tt.sz, tt.l, tt.b); r != tt.wsz || b != tt.wb {
			t.Errorf("BinDownOf(%d,%d,%d)=(%d,%d), want (%d,%d)", tt.sz, tt.l, tt.b, r, b, tt.wsz, tt.wb)
		}
	}
}

func TestBins(t *testing.T) {
	var tests = []struct {
		m, l, s uint64
	}{
		{1024, 4, 2},
		{1024, 4, 4},
		{1024, 5, 2},
		{1024, 5, 3},
		{1024, 5, 4},
		{1024, 6, 2},
		{1024, 6, 3},
		{1024, 6, 4},
		{1024, 6, 5},
	}

	for _, tt := range tests {
		var bins []uint64
		var prev uint64 = ^uint64(0)
		for i := uint64(0); i < tt.m; i++ {
			r, _ := BinOf(uint64(i), tt.l, tt.s)
			if r != prev {
				bins = append(bins, r)
				prev = r
			}
		}

		b := Bins(tt.m, tt.l, tt.s)

		t.Logf("Bins(%v,%v,%v)=%v", tt.m, tt.l, tt.s, b)

		if !reflect.DeepEqual(b, bins) {
			t.Errorf("Bins(%v,%v,%v)=%v, want %v\n", tt.m, tt.l, tt.s, b, bins)
		}
	}
}
