package tsz

import (
	"math/rand"
	"strconv"
	"testing"
)

var T uint32
var V float64

func BenchmarkPushSeries4h(b *testing.B) {
	s := NewSeries4h(0)
	N := uint32(b.N)
	for i := uint32(1); i <= N; i++ {
		if i%10 == 0 {
			s.Push(i, 0)
		} else if i%10 == 1 {
			s.Push(i, 1)
		} else {
			s.Push(i, float64(i)+123.45)
		}
	}
	s.Finish()
	b.Logf("Series4h size: %dB", len(s.Bytes()))
}

var benchmarkSeriesLongChunkSizes = []int{1, 10, 30, 60, 120, 180, 240}

func benchmarkSeriesLong(num int, generator func(uint32) float64) func(b *testing.B) {
	return func(b *testing.B) {
		var s *SeriesLong
		for j := 0; j < b.N; j++ {
			s = NewSeriesLong(0)
			N := uint32(num)
			for i := uint32(1); i <= N; i++ {
				s.Push(i, generator(i))
			}
			s.Finish()
		}
		b.Logf("SeriesLong size: %d points in %dB, avg %.2f bytes/point, b.n %d", num, len(s.Bytes()), float64(len(s.Bytes()))/float64(num), b.N)
	}
}

func BenchmarkPushSeriesLongMonotonicIncreaseWithResets(b *testing.B) {
	for _, num := range benchmarkSeriesLongChunkSizes {
		b.Run(strconv.Itoa(num), benchmarkSeriesLong(num, func(i uint32) float64 {
			if i%10 == 0 {
				return 0
			} else if i%10 == 1 {
				return 1
			}
			return float64(i) + 123.45
		}))
	}
}

func BenchmarkPushSeriesLongMonotonicIncrease(b *testing.B) {
	for _, num := range benchmarkSeriesLongChunkSizes {
		b.Run(strconv.Itoa(num), benchmarkSeriesLong(num, func(i uint32) float64 {
			return float64(i) + 123.45
		}))
	}
}

func BenchmarkPushSeriesLongSawtooth(b *testing.B) {
	for _, num := range benchmarkSeriesLongChunkSizes {
		b.Run(strconv.Itoa(num), benchmarkSeriesLong(num, func(i uint32) float64 {
			multiplier := 1.0
			if i%2 == 0 {
				multiplier = -1.0
			}
			return multiplier*123.45 + float64(i)/1000
		}))
	}
}

func BenchmarkPushSeriesLongSawtoothWithFlats(b *testing.B) {
	for _, num := range benchmarkSeriesLongChunkSizes {
		b.Run(strconv.Itoa(num), benchmarkSeriesLong(num, func(i uint32) float64 {
			multiplier := 1.0
			if i%2 == 0 && i%100 != 0 {
				multiplier = -1.0
			}
			return multiplier*123.45 + float64(i)/1000
		}))
	}
}

func BenchmarkPushSeriesLongSteps(b *testing.B) {
	for _, num := range benchmarkSeriesLongChunkSizes {
		b.Run(strconv.Itoa(num), benchmarkSeriesLong(num, func(i uint32) float64 {
			multiplier := 1.0
			if (i/100)%2 == 0 {
				multiplier = -1.0
			}
			return multiplier*123.45 + float64(i)/1000
		}))
	}
}

func BenchmarkPushSeriesLongRealWorldCPU(b *testing.B) {
	values := []float64{95.3, 95.7, 86.2, 95.0, 94.7, 95.4, 94.5, 94.0, 94.7, 95.0, 95.0, 93.8, 95.3, 95.4, 94.6, 83.8, 94.5, 94.5, 94.6, 92.0, 95.0, 89.6, 72.8, 72.1, 86.5, 94.9, 94.9, 93.9, 94.4, 95.4, 95.1, 93.7, 95.5, 95.4, 94.4, 93.2, 94.6, 95.5, 94.9, 94.1, 95.0, 95.5, 94.7, 93.7, 95.1, 96.6, 95.3, 94.0, 95.0, 95.2, 93.3, 94.2, 95.2, 94.9, 94.5, 95.3, 93.2, 95.4, 95.0, 95.2, 93.7}
	for _, num := range benchmarkSeriesLongChunkSizes {
		b.Run(strconv.Itoa(num), benchmarkSeriesLong(num, func(i uint32) float64 {
			return values[int(i)%len(values)]
		}))
	}
}

func BenchmarkIterSeries4h(b *testing.B) {
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

func BenchmarkIterSeriesLong(b *testing.B) {
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

func BenchmarkIterSeriesLongInterface(b *testing.B) {
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
