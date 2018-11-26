package jump

import (
	"testing"
)

func TestHash(t *testing.T) {

	tests := []struct {
		key    uint64
		bucket []int32
	}{
		// Generated from the reference C++ code
		{0, []int32{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}},
		{1, []int32{0, 0, 0, 0, 0, 0, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 17, 17}},
		{0xdeadbeef, []int32{0, 1, 2, 3, 3, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 16, 16, 16}},
		{0x0ddc0ffeebadf00d, []int32{0, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 15, 15, 15, 15}},
	}

	for _, tt := range tests {
		for i, v := range tt.bucket {
			if got := Hash(tt.key, i+1); got != v {
				t.Errorf("Hash(%v, %v)=%v, want %v", tt.key, i+1, got, v)
			}
		}
	}

}

// From Guava
func TestGolden(t *testing.T) {

	golden100 := []int32{0, 55, 62, 8, 45, 59, 86, 97, 82, 59, 73, 37, 17, 56, 86, 21, 90, 37, 38, 83}
	for i, v := range golden100 {
		if g := Hash(uint64(i), 100); g != v {
			t.Errorf("golden100 failed: Hash(%v, 100)=%v, want %v\n", i, g, v)

		}
	}

	var tests = []struct {
		k       uint64
		buckets int
		out     int32
	}{
		{10863919174838991, 11, 6},
		{2016238256797177309, 11, 3},
		{1673758223894951030, 11, 5},
		{2, 100001, 80343},
		{2201, 100001, 22152},
		{2202, 100001, 15018},
	}

	for _, tt := range tests {
		if g := Hash(tt.k, tt.buckets); g != tt.out {
			t.Errorf("compat failed: Hash(%v, %v)=%v, want %v\n", tt.k, tt.buckets, g, tt.out)

		}
	}
}
