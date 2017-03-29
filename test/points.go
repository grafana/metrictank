// package test contains utility functions used by tests/benchmarks in various packages
package test

import (
	"math"
	"math/rand"

	"gopkg.in/raintank/schema.v1"
)

// these serve as a "cache" of clean points we can use instead of regenerating all the time
var randFloatsWithNullsBuf []schema.Point
var randFloatsBuf []schema.Point

func RandFloats1M() []schema.Point {
	if len(randFloatsBuf) == 0 {
		// let's just do the "odd" case, since the non-odd will be sufficiently close
		randFloatsBuf = make([]schema.Point, 1000001)
		for i := 0; i < len(randFloatsBuf); i++ {
			randFloatsBuf[i] = schema.Point{Val: rand.Float64(), Ts: uint32(i)}
		}
	}
	out := make([]schema.Point, len(randFloatsBuf))
	copy(out, randFloatsBuf)
	return out
}

func RandFloatsWithNulls1M() []schema.Point {
	if len(randFloatsWithNullsBuf) == 0 {
		// let's just do the "odd" case, since the non-odd will be sufficiently close
		randFloatsWithNullsBuf = make([]schema.Point, 1000001)
		for i := 0; i < len(randFloatsWithNullsBuf); i++ {
			if i%2 == 0 {
				randFloatsWithNullsBuf[i] = schema.Point{Val: math.NaN(), Ts: uint32(i)}
			} else {
				randFloatsWithNullsBuf[i] = schema.Point{Val: rand.Float64(), Ts: uint32(i)}
			}
		}
	}
	out := make([]schema.Point, len(randFloatsBuf))
	copy(out, randFloatsWithNullsBuf)
	return out
}
