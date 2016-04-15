package main

import (
	"github.com/raintank/raintank-metric/schema"
	"testing"
)

func TestJsonMarshal(t *testing.T) {
	cases := []struct {
		in  []Series
		out string
	}{
		{
			in:  []Series{},
			out: `[]`,
		},
		{
			in: []Series{
				{
					Target:     "a",
					Datapoints: []schema.Point{},
					Interval:   60,
				},
			},
			out: `[{"Target":"a","Datapoints":[],"Interval":60}]`,
		},
		{
			in: []Series{
				{
					Target: "a",
					Datapoints: []schema.Point{
						{123, 60},
						{10000, 120},
						{0, 180},
						{1, 240},
					},
					Interval: 60,
				},
			},
			out: `[{"Target":"a","Datapoints":[[123.000,60],[10000.000,120],[0.000,180],[1.000,240]],"Interval":60}]`,
		},
		{
			in: []Series{
				{
					Target: "a",
					Datapoints: []schema.Point{
						{123, 60},
						{10000, 120},
						{0, 180},
						{1, 240},
					},
					Interval: 60,
				},
				{
					Target: "foo(bar)",
					Datapoints: []schema.Point{
						{123.456, 10},
						{123.7, 20},
						{124.10, 30},
						{125.0, 40},
						{126.0, 50},
					},
					Interval: 10,
				},
			},
			out: `[{"Target":"a","Datapoints":[[123.000,60],[10000.000,120],[0.000,180],[1.000,240]],"Interval":60},{"Target":"foo(bar)","Datapoints":[[123.456,10],[123.700,20],[124.100,30],[125.000,40],[126.000,50]],"Interval":10}]`,
		},
	}
	js := bufPool.Get().([]byte)
	for _, c := range cases {
		js, err := graphiteRaintankJSON(js[:0], c.in)
		if err != nil {
			panic(err)
		}
		got := string(js)
		if c.out != got {
			t.Fatalf("bad json output.\nexpected:%s\ngot:     %s\n", c.out, got)
		}
	}
	bufPool.Put(js[:0])
}

func BenchmarkSeriesJson(b *testing.B) {
	pA := make([]schema.Point, 1000, 1000)
	baseTs := 1500000000
	for i := 0; i < 1000; i++ {
		pA[i] = schema.Point{Val: float64(10000 * i), Ts: uint32(baseTs + 10*i)}
	}
	data := []Series{
		Series{
			Target:     "some.metric.with.a-whole-bunch-of.integers",
			Datapoints: pA,
			Interval:   10,
		},
	}
	b.SetBytes(int64(len(pA) * 12))

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		js := bufPool.Get().([]byte)
		js, err := graphiteRaintankJSON(js, data)
		if err != nil || len(js) < 1000 {
			panic(err)
		}
		bufPool.Put(js[:0])
	}
}

func BenchmarkHttpRespJson(b *testing.B) {
	pA := make([]schema.Point, 1000, 1000)
	pB := make([]schema.Point, 1000, 1000)
	baseTs := 1500000000
	for i := 0; i < 1000; i++ {
		pA[i] = schema.Point{Val: float64(10000 * i), Ts: uint32(baseTs + 10*i)}
		pB[i] = schema.Point{Val: 12.34 * float64(i), Ts: uint32(baseTs + 10*i)}
	}
	data := []Series{
		{
			Target:     "some.metric.with.a-whole-bunch-of.integers",
			Datapoints: pA,
			Interval:   10,
		},
		{
			Target:     "some.metric.with.a-whole-bunch-of.floats",
			Datapoints: pB,
			Interval:   10,
		},
	}
	b.SetBytes(int64(len(pA) * 2 * 12))
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		js := bufPool.Get().([]byte)
		js, err := graphiteRaintankJSON(js, data)
		if err != nil || len(js) < 1000 {
			panic(err)
		}
		bufPool.Put(js[:0])
	}
}
