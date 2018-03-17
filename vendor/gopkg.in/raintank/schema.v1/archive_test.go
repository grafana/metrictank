package schema

import "testing"

func TestArchive(t *testing.T) {
	type c struct {
		method     Method
		span       uint32
		expArchive Archive
		expStr     string
	}
	cases := []c{
		{Avg, 15, 0x31, "avg_15"},
		{Cnt, 120, 0x76, "cnt_120"},
	}
	for i, cas := range cases {
		arch := NewArchive(cas.method, cas.span)
		if arch != cas.expArchive {
			t.Fatalf("case %d: expected archive %d, got %d", i, cas.expArchive, arch)
		}
		str := arch.String()
		if str != cas.expStr {
			t.Fatalf("case %d: expected string %q, got %q", i, cas.expStr, str)
		}
	}
}
