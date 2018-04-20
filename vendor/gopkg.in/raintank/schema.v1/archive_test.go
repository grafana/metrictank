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
		{Avg, 15, 0x301, "avg_15"},
		{Cnt, 120, 0x706, "cnt_120"},
		{Cnt, 3600, 0xF06, "cnt_3600"},
		{Lst, 7200, 0x1103, "lst_7200"},
		{Min, 6 * 3600, 0x1505, "min_21600"},
		{Cnt, 2, 0x6, "cnt_2"},
		{Avg, 5, 0x101, "avg_5"},
		{Cnt, 3600 + 30*60, 0x1006, "cnt_5400"},
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
