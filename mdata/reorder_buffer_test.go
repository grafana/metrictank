package mdata

import (
	"reflect"
	"testing"

	"gopkg.in/raintank/schema.v1"
)

func testAddAndGet(t *testing.T, reorderWindow uint32, testData, expectedData []schema.Point, expectAdded, expectAddFail, expectReordered uint32) []schema.Point {
	var flushed []schema.Point
	b := NewReorderBuffer(reorderWindow, 1)
	metricsTooOld.SetUint32(0)
	metricsReordered.SetUint32(0)
	addedCount := uint32(0)
	for _, point := range testData {
		addRes, accepted := b.Add(point.Ts, point.Val)
		flushed = append(flushed, addRes...)
		if accepted {
			addedCount++
		}
	}
	if expectAddFail != metricsTooOld.Peek() {
		t.Fatalf("Expected %d failures, but had %d", expectAddFail, metricsTooOld.Peek())
	}

	if expectAdded != addedCount {
		t.Fatalf("Expected %d accepted datapoints, but had %d", expectAdded, addedCount)
	}

	if metricsReordered.Peek() != expectReordered {
		t.Fatalf("Expected %d metrics to get reordered, but had %d", expectReordered, metricsReordered.Peek())
	}
	returned := b.Get()

	if !reflect.DeepEqual(expectedData, returned) {
		t.Fatalf("Returned data does not match expected data\n%+v\n %+v", testData, expectedData)
	}
	return flushed
}

// mixes up a sorted slice, but only within a certain range
// it simply reverses the order of bunches of a fixed size that's defined by the unsortBy parameter
// for example if unsortBy is 3, then this:
// [0,1,2,3,4,5,6,7,8,9]
// will be turned into this:
// [2,1,0,5,4,3,8,7,6,9]
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

func TestROBUnsort(t *testing.T) {
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

func TestROBAddAndGetInOrder(t *testing.T) {
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
	testAddAndGet(t, 600, testData, expectedData, 3, 0, 0)
}

func TestROBAddAndGetInReverseOrderOutOfWindow(t *testing.T) {
	testData := []schema.Point{
		{Ts: 1003, Val: 300},
		{Ts: 1002, Val: 200},
		{Ts: 1001, Val: 100},
	}
	expectedData := []schema.Point{
		{Ts: 1003, Val: 300},
	}
	testAddAndGet(t, 1, testData, expectedData, 1, 2, 0)
}

func TestROBAddAndGetOutOfOrderInsideWindow(t *testing.T) {
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
	testAddAndGet(t, 600, testData, expectedData, 9, 0, 2)
}

func TestROBAddAndGetOutOfOrderInsideWindowAsFirstPoint(t *testing.T) {
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
	testAddAndGet(t, 600, testData, expectedData, 9, 0, 3)
}

func TestROBOmitFlushIfNotEnoughData(t *testing.T) {
	b := NewReorderBuffer(9, 1)
	for i := uint32(1); i < 10; i++ {
		flushed, _ := b.Add(i, float64(i*100))
		if len(flushed) > 0 {
			t.Fatalf("Expected no data to get flushed out")
		}
	}
}

func TestROBAddAndGetOutOfOrderOutOfWindow(t *testing.T) {
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
	// point 2 should be missing because out of reorder window
	expectedFlushedData := []schema.Point{
		{Ts: 1001, Val: 100},
		{Ts: 1003, Val: 300},
		{Ts: 1004, Val: 400},
	}
	flushedData := testAddAndGet(t, 5, testData, expectedData, 8, 1, 2)
	if !reflect.DeepEqual(flushedData, expectedFlushedData) {
		t.Fatalf("Flushed data does not match expected flushed data:\n%+v\n%+v", flushedData, expectedFlushedData)
	}
}

func TestROBFlushSortedData(t *testing.T) {
	var results []schema.Point
	buf := NewReorderBuffer(600, 1)
	metricsTooOld.SetUint32(0)
	for i := 1100; i < 2100; i++ {
		flushed, accepted := buf.Add(uint32(i), float64(i))
		if metricsTooOld.Peek() != 0 || !accepted {
			t.Fatalf("Adding failed")
		}
		results = append(results, flushed...)
	}

	for i := 0; i < 400; i++ {
		if results[i].Ts != uint32(i+1100) || results[i].Val != float64(i+1100) {
			t.Fatalf("Unexpected results %+v", results)
		}
	}
}

func TestROBFlushUnsortedData1(t *testing.T) {
	var results []schema.Point
	buf := NewReorderBuffer(3, 1)
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
	metricsTooOld.SetUint32(0)
	for _, p := range data {
		flushed, accepted := buf.Add(p.Ts, p.Val)
		if metricsTooOld.Peek() != 0 || !accepted {
			failedCount++
			metricsTooOld.SetUint32(0)
		} else {
			results = append(results, flushed...)
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

func TestROBFlushUnsortedData2(t *testing.T) {
	var results []schema.Point
	buf := NewReorderBuffer(600, 1)
	data := make([]schema.Point, 1000)
	for i := 0; i < 1000; i++ {
		data[i] = schema.Point{Ts: uint32(i + 1000), Val: float64(i + 1000)}
	}
	unsortedData := unsort(data, 10)
	for i := 0; i < len(data); i++ {
		flushed, _ := buf.Add(unsortedData[i].Ts, unsortedData[i].Val)
		results = append(results, flushed...)
	}
	for i := 0; i < 400; i++ {
		if results[i].Ts != uint32(i+1000) || results[i].Val != float64(i+1000) {
			t.Fatalf("Unexpected results %+v", results)
		}
	}
}

func TestROBFlushAndIsEmpty(t *testing.T) {
	buf := NewReorderBuffer(10, 1)

	if !buf.IsEmpty() {
		t.Fatalf("Expected IsEmpty() to be true")
	}

	buf.Add(123, 123)
	if buf.IsEmpty() {
		t.Fatalf("Expected IsEmpty() to be false")
	}

	buf.Reset()
	if !buf.IsEmpty() {
		t.Fatalf("Expected IsEmpty() to be true")
	}
}

func NewInputData(num int) []schema.Point {
	ret := make([]schema.Point, num)
	for i := 1; i <= num; i++ {
		ret[i] = schema.Point{
			Val: float64(i),
			Ts:  uint32(i),
		}
	}
	return ret
}

func BenchmarkROBAddInOrder(b *testing.B) {
	data := NewInputData(b.N)
	buf := NewReorderBuffer(uint32(b.N), 1)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		buf.Add(data[i].Ts, data[i].Val)
	}
}

func BenchmarkROBAddOutOfOrder(b *testing.B) {
	data := unsort(NewInputData(b.N), 10)
	buf := NewReorderBuffer(uint32(b.N), 1)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		buf.Add(data[i].Ts, data[i].Val)
	}
}

func benchmarkAddAndFlushX(b *testing.B, datapoints, reorderWindow uint32) {
	buf := NewReorderBuffer(
		reorderWindow,
		1,
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

func BenchmarkROBAddAndFlush10000(b *testing.B) {
	benchmarkAddAndFlushX(b, 10000, 1000)
}

func BenchmarkROBAddAndFlush1000(b *testing.B) {
	benchmarkAddAndFlushX(b, 1000, 100)
}

func BenchmarkROBAddAndFlush100(b *testing.B) {
	benchmarkAddAndFlushX(b, 100, 10)
}
