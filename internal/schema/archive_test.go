package schema

import (
	"testing"
)

func TestZeroArchive(t *testing.T) {
	var arch Archive // zero value
	method := arch.Method()
	if method != 0 {
		t.Fatalf("expected method 0, got %v", method)
	}
	span := arch.Span()
	if span != 0 {
		t.Fatalf("expected span 0, got %v", span)
	}
}

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
		method := arch.Method()
		if method != cas.method {
			t.Fatalf("case %d: expected method %v, got %v", i, cas.method, method)
		}
		span := arch.Span()
		if span != cas.span {
			t.Fatalf("case %d: expected span %v, got %v", i, cas.span, span)
		}
	}
}

func TestArchiveFromString(t *testing.T) {
	cases := []struct {
		str        string
		expErr     bool
		expArchive Archive
	}{
		{"_deeff", true, 0},
		{"_foo_bar_", true, 0},
		{"_sum_1800", true, 0},
		{"_SUM_1800", true, 0},
		{"sum_1800", false, NewArchive(Sum, 1800)},
		{"sum_1801", true, 0},
		{"SUM_1800", true, 0},
		{"min_600", false, NewArchive(Min, 600)},
	}

	for i, c := range cases {
		archive, err := ArchiveFromString(c.str)
		if (err != nil) != c.expErr {
			t.Fatalf("case %d exp err %v got %v", i, c.expErr, err)
		}
		if err != nil {
			continue
		}
		if archive != c.expArchive {
			t.Fatalf("case %d exp Archive %v got %v", i, c.expArchive, archive)
		}
	}
}
