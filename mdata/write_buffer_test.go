package mdata

import (
	"fmt"
	"testing"

	"github.com/raintank/metrictank/conf"
	"gopkg.in/raintank/schema.v1"
)

func testAddAndGet(t *testing.T, conf *conf.WriteBufferConf, collector func(uint32, float64), testData, expectedData []schema.Point, expectAddFail bool) {
	b := NewWriteBuffer(conf, collector)
	gotFailure := false
	for _, point := range testData {
		success := b.Add(point.Ts, point.Val)
		if !success {
			gotFailure = true
		}
		b.flushIfReady()
	}
	if expectAddFail && !gotFailure {
		t.Fatal("Expected an add to fail, but they all succeeded")
	}
	returned := b.Get()

	if len(expectedData) != len(returned) {
		t.Fatal("Length of returned and testData data unequal")
	}
	if !pointSlicesAreEqual(expectedData, returned) {
		t.Fatal(fmt.Sprintf("Returned data does not match testData data %+v, %+v", testData, returned))
	}
}

func pointSlicesAreEqual(a, b []schema.Point) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if (a[i].Ts != b[i].Ts) || (a[i].Val != b[i].Val) {
			return false
		}
	}
	return true
}

func unsort(data []schema.Point, unsortBy int) []schema.Point {
	out := make([]schema.Point, len(data))
	i := 0
	for ; i < len(data)-unsortBy; i = i + unsortBy {
		for j := 0; j < unsortBy; j++ {
			out[i+j] = data[i+unsortBy-j-1]
		}
	}
	for ; i < len(data); i++ {
		out[i] = data[i]
	}
	return out
}

func TestUnsort(t *testing.T) {
	testData := []schema.Point{
		{Ts: 0, Val: 0},
		{Ts: 1, Val: 100},
		{Ts: 2, Val: 200},
		{Ts: 3, Val: 300},
		{Ts: 4, Val: 400},
		{Ts: 5, Val: 500},
		{Ts: 6, Val: 600},
		{Ts: 7, Val: 700},
		{Ts: 8, Val: 800},
		{Ts: 9, Val: 900},
	}
	expectedData := []schema.Point{
		{Ts: 2, Val: 200},
		{Ts: 1, Val: 100},
		{Ts: 0, Val: 0},
		{Ts: 5, Val: 500},
		{Ts: 4, Val: 400},
		{Ts: 3, Val: 300},
		{Ts: 8, Val: 800},
		{Ts: 7, Val: 700},
		{Ts: 6, Val: 600},
		{Ts: 9, Val: 900},
	}
	unsortedData := unsort(testData, 3)

	for i := 0; i < len(expectedData); i++ {
		if unsortedData[i] != expectedData[i] {
			t.Fatalf("unsort function returned unexpected data %+v", unsortedData)
		}
	}
}

func TestAddAndGetInOrder(t *testing.T) {
	testData := []schema.Point{
		{Ts: 1, Val: 100},
		{Ts: 2, Val: 200},
		{Ts: 3, Val: 300},
	}
	expectedData := []schema.Point{
		{Ts: 1, Val: 100},
		{Ts: 2, Val: 200},
		{Ts: 3, Val: 300},
	}
	conf := &conf.WriteBufferConf{ReorderWindow: 600, FlushMin: 1}
	testAddAndGet(t, conf, nil, testData, expectedData, false)
}

func TestAddAndGetInReverseOrderOutOfWindow(t *testing.T) {
	testData := []schema.Point{
		{Ts: 3, Val: 300},
		{Ts: 2, Val: 200},
		{Ts: 1, Val: 100},
	}
	expectedData := []schema.Point{
		{Ts: 3, Val: 300},
	}
	conf := &conf.WriteBufferConf{ReorderWindow: 1, FlushMin: 1}
	collector := func(ts uint32, val float64) {}
	testAddAndGet(t, conf, collector, testData, expectedData, true)
}

func TestAddAndGetOutOfOrderInsideWindow(t *testing.T) {
	testData := []schema.Point{
		{Ts: 1, Val: 100},
		{Ts: 2, Val: 200},
		{Ts: 4, Val: 400},
		{Ts: 3, Val: 300},
		{Ts: 5, Val: 500},
		{Ts: 6, Val: 600},
		{Ts: 8, Val: 800},
		{Ts: 7, Val: 700},
		{Ts: 9, Val: 900},
	}
	expectedData := []schema.Point{
		{Ts: 1, Val: 100},
		{Ts: 2, Val: 200},
		{Ts: 3, Val: 300},
		{Ts: 4, Val: 400},
		{Ts: 5, Val: 500},
		{Ts: 6, Val: 600},
		{Ts: 7, Val: 700},
		{Ts: 8, Val: 800},
		{Ts: 9, Val: 900},
	}
	conf := &conf.WriteBufferConf{ReorderWindow: 600, FlushMin: 1}
	testAddAndGet(t, conf, nil, testData, expectedData, false)
}

func TestAddAndGetOutOfOrderInsideWindowAsFirstPoint(t *testing.T) {
	testData := []schema.Point{
		{Ts: 2, Val: 200},
		{Ts: 4, Val: 400},
		{Ts: 3, Val: 300},
		{Ts: 5, Val: 500},
		{Ts: 1, Val: 100},
		{Ts: 6, Val: 600},
		{Ts: 8, Val: 800},
		{Ts: 7, Val: 700},
		{Ts: 9, Val: 900},
	}
	expectedData := []schema.Point{
		{Ts: 1, Val: 100},
		{Ts: 2, Val: 200},
		{Ts: 3, Val: 300},
		{Ts: 4, Val: 400},
		{Ts: 5, Val: 500},
		{Ts: 6, Val: 600},
		{Ts: 7, Val: 700},
		{Ts: 8, Val: 800},
		{Ts: 9, Val: 900},
	}
	conf := &conf.WriteBufferConf{ReorderWindow: 600, FlushMin: 1}
	testAddAndGet(t, conf, nil, testData, expectedData, false)
}

func TestOmitFlushIfNotEnoughData(t *testing.T) {
	conf := &conf.WriteBufferConf{ReorderWindow: 9, FlushMin: 1}
	collector := func(ts uint32, val float64) {
		t.Fatalf("Expected the flush function to not get called")
	}
	b := NewWriteBuffer(conf, collector)
	for i := uint32(1); i < 10; i++ {
		b.Add(i, float64(i*100))
	}
	b.flushIfReady()
}

func TestAddAndGetOutOfOrderOutOfWindow(t *testing.T) {
	testData := []schema.Point{
		{Ts: 1, Val: 100},
		{Ts: 4, Val: 400},
		{Ts: 3, Val: 300},
		{Ts: 5, Val: 500},
		{Ts: 6, Val: 600},
		{Ts: 8, Val: 800},
		{Ts: 7, Val: 700},
		{Ts: 9, Val: 900},
		{Ts: 2, Val: 200},
	}
	expectedData := []schema.Point{
		{Ts: 5, Val: 500},
		{Ts: 6, Val: 600},
		{Ts: 7, Val: 700},
		{Ts: 8, Val: 800},
		{Ts: 9, Val: 900},
	}
	flushedData := []schema.Point{}
	// point 2 should be missing because out of reorder window
	expectedFlushedData := []schema.Point{
		{Ts: 1, Val: 100},
		{Ts: 3, Val: 300},
		{Ts: 4, Val: 400},
	}
	conf := &conf.WriteBufferConf{ReorderWindow: 5, FlushMin: 1}
	collector := func(ts uint32, val float64) {
		flushedData = append(flushedData, schema.Point{Ts: ts, Val: val})
	}
	testAddAndGet(t, conf, collector, testData, expectedData, true)
	if !pointSlicesAreEqual(flushedData, expectedFlushedData) {
		t.Fatal(fmt.Sprintf("Flushed data does not match expected flushed data: %+v %+v", flushedData, expectedFlushedData))
	}
}

func TestFlushSortedData(t *testing.T) {
	resultI := 0
	results := make([]schema.Point, 400)
	receiver := func(ts uint32, val float64) {
		results[resultI] = schema.Point{Ts: ts, Val: val}
		resultI++
	}
	buf := NewWriteBuffer(&conf.WriteBufferConf{ReorderWindow: 600, FlushMin: 1}, receiver)
	for i := 100; i < 1100; i++ {
		buf.Add(uint32(i), float64(i))
	}

	buf.flushIfReady()
	for i := 0; i < 400; i++ {
		if results[i].Ts != uint32(i+100) || results[i].Val != float64(i+100) {
			t.Fatalf("Unexpected results %+v", results)
		}
	}
}

func TestFlushUnsortedData(t *testing.T) {
	resultI := 0
	results := make([]schema.Point, 400)
	receiver := func(ts uint32, val float64) {
		results[resultI] = schema.Point{Ts: ts, Val: val}
		resultI++
	}
	buf := NewWriteBuffer(&conf.WriteBufferConf{ReorderWindow: 600, FlushMin: 1}, receiver)
	data := make([]schema.Point, 1000)
	for i := 0; i < 1000; i++ {
		data[i] = schema.Point{Ts: uint32(i + 100), Val: float64(i + 100)}
	}
	unsortedData := unsort(data, 10)
	for i := 0; i < len(data); i++ {
		buf.Add(unsortedData[i].Ts, unsortedData[i].Val)
	}
	buf.flushIfReady()
	for i := 0; i < 400; i++ {
		if results[i].Ts != uint32(i+100) || results[i].Val != float64(i+100) {
			t.Fatalf("Unexpected results %+v", results)
		}
	}
}

func BenchmarkAddInOrder(b *testing.B) {
	data := make([]schema.Point, b.N)
	buf := NewWriteBuffer(&conf.WriteBufferConf{ReorderWindow: uint32(b.N), FlushMin: 1}, nil)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		buf.Add(data[i].Ts, data[i].Val)
	}
}

func BenchmarkAddOutOfOrder(b *testing.B) {
	data := make([]schema.Point, b.N)
	unsortedData := unsort(data, 10)
	buf := NewWriteBuffer(&conf.WriteBufferConf{ReorderWindow: uint32(b.N), FlushMin: 1}, nil)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		buf.Add(unsortedData[i].Ts, unsortedData[i].Val)
	}
}

func benchmarkAddAndFlushX(b *testing.B, datapoints, flushMin, reorderWindow uint32) {
	buf := NewWriteBuffer(
		&conf.WriteBufferConf{ReorderWindow: reorderWindow, FlushMin: flushMin},
		func(ts uint32, val float64) {},
	)
	ts := uint32(1)
	for ; ts <= datapoints; ts++ {
		buf.Add(ts, float64(ts*100))
	}
	buf.flushIfReady()

	b.ResetTimer()

	for run := 0; run < b.N; run++ {
		ts := uint32(1)
		for ; ts <= datapoints; ts++ {
			buf.Add(ts, float64(ts*100))
		}
		buf.flushIfReady()
	}
}

func BenchmarkAddAndFlush10000(b *testing.B) {
	benchmarkAddAndFlushX(b, 10000, 100, 1000)
}

func BenchmarkAddAndFlush1000(b *testing.B) {
	benchmarkAddAndFlushX(b, 1000, 10, 100)
}

func BenchmarkAddAndFlush100(b *testing.B) {
	benchmarkAddAndFlushX(b, 100, 1, 10)
}
