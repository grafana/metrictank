package main

import "testing"

type matchCase struct {
	name  string
	match bool
}

func TestMatchSimpleAsterisk(t *testing.T) {
	patt := "foo.*.bar"
	matchCases := []matchCase{
		{"foo", false},
		{"bar", false},
		{"foo.bar", false},
		{"foo.abc.bar", true},
		{"fooa.abc.bar", false},
		{"foo.abc.cbar", false},
		{"foo.abc.def.bar", false},
	}
	testmatch(patt, matchCases, t)
}

func TestMatchDoubleAsterisk(t *testing.T) {
	patt := "foo.*.bar.*"
	matchCases := []matchCase{
		{"foo", false},
		{"bar", false},
		{"foo.bar", false},
		{"foo.abc.bar", false},
		{"foo.bar.baz", false},
		{"foo.abc.bar.a", true},
		{"fooa.abc.bar.a", false},
		{"foo.abc.cbar.a", false},
		{"foo.abc.def.bar.a", false},
	}
	testmatch(patt, matchCases, t)
}

func testmatch(patt string, matchCases []matchCase, t *testing.T) {
	for _, mc := range matchCases {
		metric := Metric{
			name: mc.name,
		}
		ok := match("", "", patt, metric)
		if ok != mc.match {
			t.Fatalf("case match('','',%q, Metric{name:%q}) -> expected %t, got %t", patt, mc.name, mc.match, ok)
		}
	}
}
