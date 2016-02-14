package main

import (
	"testing"
)

func TestJsonMarshal(t *testing.T) {
	data := []Series{
		{
			Target: "a",
			Datapoints: []Point{
				{123, 60},
				{10000, 120},
				{0, 180},
				{1, 240},
			},
			Interval: 60,
		},
		{
			Target: "foo(bar)",
			Datapoints: []Point{
				{123.456, 10},
				{123.7, 20},
				{124.10, 30},
				{125.0, 40},
				{126.0, 50},
			},
			Interval: 10,
		},
	}
	js, err := graphiteJSON(data)
	if err != nil {
		panic(err)
	}
	exp := `[{"Target":"a","Datapoints":[[123.000,60],[10000.000,120],[0.000,180],[1.000,240]],"Interval":60},{"Target":"foo(bar)","Datapoints":[[123.456,10],[123.700,20],[124.100,30],[125.000,40],[126.000,50]],"Interval":10}]`
	got := string(js)
	if exp != got {
		t.Fatalf("bad json output.\nexpected:%s\ngot:     %s\n", exp, got)
	}
}

func BenchmarkSeriesJson(b *testing.B) {
	pA := make([]Point, 1000, 1000)
	baseTs := 1500000000
	for i := 0; i < 1000; i++ {
		pA[i] = Point{float64(10000 * i), uint32(baseTs + 10*i)}
	}
	data := []Series{
		Series{
			Target:     "some.metric.with.a-whole-bunch-of.integers",
			Datapoints: pA,
			Interval:   10,
		},
	}

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		js, err := graphiteJSON(data)
		if err != nil || len(js) < 1000 {
			panic(err)
		}
	}
}

func BenchmarkHttpRespJson(b *testing.B) {
	pA := make([]Point, 1000, 1000)
	pB := make([]Point, 1000, 1000)
	baseTs := 1500000000
	for i := 0; i < 1000; i++ {
		pA[i] = Point{float64(10000 * i), uint32(baseTs + 10*i)}
		pB[i] = Point{12.34 * float64(i), uint32(baseTs + 10*i)}
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

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		js, err := graphiteJSON(data)
		if err != nil || len(js) < 1000 {
			panic(err)
		}
	}
}
