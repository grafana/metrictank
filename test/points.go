// package test contains utility functions used by tests/benchmarks in various packages
package test

import (
	"math"
	"math/rand"

	"github.com/raintank/schema"
)

// these serve as a "cache" of clean point slices - grouped by size -
// which we can use instead of regenerating all the time
var randFloats = make(map[int][]schema.Point)
var randFloatsWithNulls = make(map[int][]schema.Point)

func RandFloats10k() []schema.Point { return RandFloats(10000) }
func RandFloats1M() []schema.Point  { return RandFloats(1000000) }

func RandFloats(size int) []schema.Point {
	data, ok := randFloats[size]
	if !ok {
		data = make([]schema.Point, size)
		for i := 0; i < size; i++ {
			data[i] = schema.Point{Val: rand.Float64(), Ts: uint32(i)}
		}
		randFloats[size] = data
	}
	out := make([]schema.Point, size)
	copy(out, data)
	return out
}

func RandFloatsWithNulls10k() []schema.Point { return RandFloatsWithNulls(10000) }
func RandFloatsWithNulls1M() []schema.Point  { return RandFloatsWithNulls(1000000) }

func RandFloatsWithNulls(size int) []schema.Point {
	data, ok := randFloatsWithNulls[size]
	if !ok {
		data = make([]schema.Point, size)
		for i := 0; i < size; i++ {
			if i%2 == 0 {
				data[i] = schema.Point{Val: math.NaN(), Ts: uint32(i)}
			} else {
				data[i] = schema.Point{Val: rand.Float64(), Ts: uint32(i)}
			}
		}
		randFloatsWithNulls[size] = data
	}
	out := make([]schema.Point, size)
	copy(out, data)
	return out
}
