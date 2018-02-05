package main

import "testing"

type testCase struct {
	now    int
	ts     int
	ttl    int // amount of seconds to retain in general
	newTTL int // amount of seconds to retain *as of now*
}

func TestGetTTL(t *testing.T) {
	cases := []testCase{
		{120, 100, 10, 1}, // should have expired 10s ago. set ttl to 1 to expire as soon as we can.
		{120, 110, 10, 1}, // time to expire it right now. set ttl to 1 to expire as soon as we can.
		{120, 115, 10, 5}, // data is 5s old, so expire 5s from now
		{120, 120, 10, 10},
		{120, 130, 10, 20}, // future data with a TS > now should expire 10s after its TS, aka 20s after now
	}
	for i, c := range cases {
		newTTL := getTTL(c.now, c.ts, c.ttl)
		if newTTL != c.newTTL {
			t.Errorf("case %d: expected %d got %d", i, c.newTTL, newTTL)
		}
	}
}

type completenessCase struct {
	token int64
	est   float64
}

func TestCompleteness(t *testing.T) {
	cases := []completenessCase{
		{-9223372036854775808, 0},
		{-9223372036854775808 / 2, 0.25},
		{0, 0.5},
		{9223372036854775807 / 3, float64(4) / float64(6)},
		{9223372036854775807, 1},
	}
	for i, c := range cases {
		est := completenessEstimate(c.token)
		if est != c.est {
			t.Errorf("case %d: expected %f got %f", i, c.est, est)
		}
	}

}
