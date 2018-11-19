package tsz

import (
	"math/rand"
	"testing"
)

var T uint32
var V float64

func BenchmarkIter4h(b *testing.B) {
	s := NewSeries4h(0)
	N := uint32(b.N)
	for i := uint32(1); i <= N; i++ {
		s.Push(i, 123.45)
	}
	b.ResetTimer()
	iter := s.Iter(1)
	var t uint32
	var v float64
	for iter.Next() {
		t, v = iter.Values()
	}
	err := iter.Err()
	if err != nil {
		panic(err)
	}
	T = t
	V = v
}

func BenchmarkIterLong(b *testing.B) {
	s := NewSeriesLong(0)
	N := uint32(b.N)
	for i := uint32(1); i <= N; i++ {
		s.Push(i, 123.45)
	}
	b.ResetTimer()
	iter := s.Iter()
	var t uint32
	var v float64
	for iter.Next() {
		t, v = iter.Values()
	}
	err := iter.Err()
	if err != nil {
		panic(err)
	}
	T = t
	V = v
}

func BenchmarkIterLongInterface(b *testing.B) {
	s := NewSeriesLong(0)
	N := uint32(b.N)
	for i := uint32(1); i <= N; i++ {
		s.Push(i, 123.45)
	}
	b.ResetTimer()
	var t uint32
	var v float64
	var iter Iter
	// avoid compiler optimization where it can statically assign the right type
	// and skip the overhead of the interface
	if rand.Intn(1) == 0 {
		iter = s.Iter()
	}
	for iter.Next() {
		t, v = iter.Values()
	}
	err := iter.Err()
	if err != nil {
		panic(err)
	}
	T = t
	V = v
}
