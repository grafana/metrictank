package mdata

import (
	"fmt"
	"testing"

	"gopkg.in/raintank/schema.v1"
)

func testAddAndGet(t *testing.T, reorderWindow uint32, flush func(uint32, float64), testData, expectedData []schema.Point, expectAddFail bool) {
	b := NewReorderBuffer(reorderWindow, 1, flush)
	gotFailure := false
	for _, point := range testData {
		success := b.Add(point.Ts, point.Val)
		if !success {
			gotFailure = true
		}
	}
	if expectAddFail && !gotFailure {
		t.Fatal("Expected an add to fail, but they all succeeded")
	}
	returned := b.Get()

	if len(expectedData) != len(returned) {
		t.Fatal("Length of returned and testData data unequal", len(returned), len(expectedData))
	}
	if !pointSlicesAreEqual(expectedData, returned) {
		t.Fatal(fmt.Sprintf("Returned data does not match expected data\n%+v\n %+v", testData, expectedData))
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

func TestWriteBufferUnsort(t *testing.T) {
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

func TestWriteBufferAddAndGetInOrder(t *testing.T) {
	testData := []schema.Point{
		{Ts: 1001, Val: 100},
		{Ts: 1002, Val: 200},
		{Ts: 1003, Val: 300},
	}
	expectedData := []schema.Point{
		{Ts: 1001, Val: 100},
		{Ts: 1002, Val: 200},
		{Ts: 1003, Val: 300},
	}
	flush := func(ts uint32, val float64) {}
	testAddAndGet(t, 600, flush, testData, expectedData, false)
}

func TestWriteBufferAddAndGetInReverseOrderOutOfWindow(t *testing.T) {
	testData := []schema.Point{
		{Ts: 1003, Val: 300},
		{Ts: 1002, Val: 200},
		{Ts: 1001, Val: 100},
	}
	expectedData := []schema.Point{
		{Ts: 1003, Val: 300},
	}
	flush := func(ts uint32, val float64) {}
	testAddAndGet(t, 1, flush, testData, expectedData, true)
}

func TestWriteBufferAddAndGetOutOfOrderInsideWindow(t *testing.T) {
	testData := []schema.Point{
		{Ts: 1001, Val: 100},
		{Ts: 1002, Val: 200},
		{Ts: 1004, Val: 400},
		{Ts: 1003, Val: 300},
		{Ts: 1005, Val: 500},
		{Ts: 1006, Val: 600},
		{Ts: 1008, Val: 800},
		{Ts: 1007, Val: 700},
		{Ts: 1009, Val: 900},
	}
	expectedData := []schema.Point{
		{Ts: 1001, Val: 100},
		{Ts: 1002, Val: 200},
		{Ts: 1003, Val: 300},
		{Ts: 1004, Val: 400},
		{Ts: 1005, Val: 500},
		{Ts: 1006, Val: 600},
		{Ts: 1007, Val: 700},
		{Ts: 1008, Val: 800},
		{Ts: 1009, Val: 900},
	}
	flush := func(ts uint32, val float64) {}
	testAddAndGet(t, 600, flush, testData, expectedData, false)
}

func TestWriteBufferAddAndGetOutOfOrderInsideWindowAsFirstPoint(t *testing.T) {
	testData := []schema.Point{
		{Ts: 1002, Val: 200},
		{Ts: 1004, Val: 400},
		{Ts: 1003, Val: 300},
		{Ts: 1005, Val: 500},
		{Ts: 1001, Val: 100},
		{Ts: 1006, Val: 600},
		{Ts: 1008, Val: 800},
		{Ts: 1007, Val: 700},
		{Ts: 1009, Val: 900},
	}
	expectedData := []schema.Point{
		{Ts: 1001, Val: 100},
		{Ts: 1002, Val: 200},
		{Ts: 1003, Val: 300},
		{Ts: 1004, Val: 400},
		{Ts: 1005, Val: 500},
		{Ts: 1006, Val: 600},
		{Ts: 1007, Val: 700},
		{Ts: 1008, Val: 800},
		{Ts: 1009, Val: 900},
	}
	flush := func(ts uint32, val float64) {}
	testAddAndGet(t, 600, flush, testData, expectedData, false)
}

func TestWriteBufferOmitFlushIfNotEnoughData(t *testing.T) {
	flush := func(ts uint32, val float64) {
		t.Fatalf("Expected the flush function to not get called")
	}
	b := NewReorderBuffer(9, 1, flush)
	for i := uint32(1); i < 10; i++ {
		b.Add(i, float64(i*100))
	}
}

func TestWriteBufferAddAndGetOutOfOrderOutOfWindow(t *testing.T) {
	testData := []schema.Point{
		{Ts: 1001, Val: 100},
		{Ts: 1004, Val: 400},
		{Ts: 1003, Val: 300},
		{Ts: 1005, Val: 500},
		{Ts: 1006, Val: 600},
		{Ts: 1008, Val: 800},
		{Ts: 1007, Val: 700},
		{Ts: 1009, Val: 900},
		{Ts: 1002, Val: 200},
	}
	expectedData := []schema.Point{
		{Ts: 1005, Val: 500},
		{Ts: 1006, Val: 600},
		{Ts: 1007, Val: 700},
		{Ts: 1008, Val: 800},
		{Ts: 1009, Val: 900},
	}
	flushedData := []schema.Point{}
	// point 2 should be missing because out of reorder window
	expectedFlushedData := []schema.Point{
		{Ts: 1001, Val: 100},
		{Ts: 1003, Val: 300},
		{Ts: 1004, Val: 400},
	}
	flush := func(ts uint32, val float64) {
		flushedData = append(flushedData, schema.Point{Ts: ts, Val: val})
	}
	testAddAndGet(t, 5, flush, testData, expectedData, true)
	if !pointSlicesAreEqual(flushedData, expectedFlushedData) {
		t.Fatal(fmt.Sprintf("Flushed data does not match expected flushed data:\n%+v\n%+v", flushedData, expectedFlushedData))
	}
}

func TestWriteBufferFlushSortedData(t *testing.T) {
	resultI := 0
	results := make([]schema.Point, 400)
	receiver := func(ts uint32, val float64) {
		results[resultI] = schema.Point{Ts: ts, Val: val}
		resultI++
	}
	buf := NewReorderBuffer(600, 1, receiver)
	for i := 1100; i < 2100; i++ {
		if !buf.Add(uint32(i), float64(i)) {
			t.Fatalf("Adding failed")
		}
	}

	for i := 0; i < 400; i++ {
		if results[i].Ts != uint32(i+1100) || results[i].Val != float64(i+1100) {
			t.Fatalf("Unexpected results %+v", results)
		}
	}
}

func TestWriteBufferFlushUnsortedData1(t *testing.T) {
	resultI := 0
	results := make([]schema.Point, 5)
	receiver := func(ts uint32, val float64) {
		results[resultI] = schema.Point{Ts: ts, Val: val}
		resultI++
	}
	metricsTooOld.SetUint32(0)
	buf := NewReorderBuffer(3, 1, receiver)
	data := []schema.Point{
		{10, 10},
		{11, 11},
		{9, 9},
		{12, 12},
		{13, 13},
		{20, 20},
		{11, 11},
		{19, 19},
	}
	failedCount := 0
	for _, p := range data {
		if !buf.Add(p.Ts, p.Val) {
			failedCount++
		}
	}
	expecting := []schema.Point{
		{9, 9},
		{10, 10},
		{11, 11},
		{12, 12},
		{13, 13},
	}
	for i := range expecting {
		if expecting[i] != results[i] {
			t.Fatalf("Unexpected results %+v, %+v", expecting, results)
		}
	}
	if failedCount != 1 {
		t.Fatalf("expecting failed count to be 1, not %d", failedCount)
	}
}

func TestWriteBufferFlushUnsortedData2(t *testing.T) {
	resultI := 0
	results := make([]schema.Point, 400)
	receiver := func(ts uint32, val float64) {
		results[resultI] = schema.Point{Ts: ts, Val: val}
		resultI++
	}
	buf := NewReorderBuffer(600, 1, receiver)
	data := make([]schema.Point, 1000)
	for i := 0; i < 1000; i++ {
		data[i] = schema.Point{Ts: uint32(i + 1000), Val: float64(i + 1000)}
	}
	unsortedData := unsort(data, 10)
	for i := 0; i < len(data); i++ {
		buf.Add(unsortedData[i].Ts, unsortedData[i].Val)
	}
	for i := 0; i < 400; i++ {
		if results[i].Ts != uint32(i+1000) || results[i].Val != float64(i+1000) {
			t.Fatalf("Unexpected results %+v", results)
		}
	}
}

func BenchmarkAddInOrder(b *testing.B) {
	data := make([]schema.Point, b.N)
	buf := NewReorderBuffer(uint32(b.N), 1, nil)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		buf.Add(data[i].Ts, data[i].Val)
	}
}

func BenchmarkAddOutOfOrder(b *testing.B) {
	data := make([]schema.Point, b.N)
	unsortedData := unsort(data, 10)
	buf := NewReorderBuffer(uint32(b.N), 1, nil)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		buf.Add(unsortedData[i].Ts, unsortedData[i].Val)
	}
}

func benchmarkAddAndFlushX(b *testing.B, datapoints, flushMin, reorderWindow uint32) {
	buf := NewReorderBuffer(
		reorderWindow,
		1,
		func(ts uint32, val float64) {},
	)
	ts := uint32(1)
	for ; ts <= datapoints; ts++ {
		buf.Add(ts, float64(ts*100))
	}

	b.ResetTimer()

	for run := 0; run < b.N; run++ {
		ts := uint32(1)
		for ; ts <= datapoints; ts++ {
			buf.Add(ts, float64(ts*100))
		}
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
