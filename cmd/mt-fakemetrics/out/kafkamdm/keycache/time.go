package keycache

import "time"

// Ref represents a unix timestamp bucketed in 10m buckets
type Ref uint32

func NewRef(t time.Time) Ref {
	unix := t.Unix()
	return Ref(unix / 600)
}

// Duration is a compact way to represent a duration
// bucketed in 10 minutely buckets.
// which allows us to cover a timeframe of 42 hours
// (just over a day and a half)
// because:
// 6 (10m periods per hour) *42 (hours) = 252 (10m periods)
type Duration uint8

// NewDuration creates a new Duration representing the
// duration between Ref and t.
// callers responsibility that t >= ref and t-ref <= 42 hours
func NewDuration(ref Ref, t time.Time) Duration {
	return Duration(NewRef(t) - ref)
}
