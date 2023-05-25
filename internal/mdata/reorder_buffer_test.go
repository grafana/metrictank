package mdata

import (
	"reflect"
	"testing"

	"github.com/grafana/metrictank/internal/mdata/errors"
	"github.com/grafana/metrictank/internal/schema"
)

func testAddAndGet(t *testing.T, reorderWindow uint32, testData, expectedData []schema.Point, expectAdded uint32, expectedErrors []error, expectReordered uint32, allowUpdate bool) []schema.Point {
	var flushed []schema.Point
	b := NewReorderBuffer(reorderWindow, 1, allowUpdate)
	metricsReordered.SetUint32(0)
	addedCount := uint32(0)
	for i, point := range testData {
		pt, addRes, err := b.Add(point.Ts, point.Val)

		if pt.Ts != 0 {
			flushed = append(flushed, pt)
			flushed = append(flushed, addRes...)
		}
		if expectedErrors != nil && err != expectedErrors[i] {
			t.Fatalf("Data point #%d: expected error '%v', but had '%v'", i, expectedErrors[i], err)
		}
		if err == nil {
			addedCount++
		}
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
	testAddAndGet(t, 600, testData, expectedData, 3, nil, 0, false)
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
	expectedErrors := []error{
		nil,
		errors.ErrMetricTooOld,
		errors.ErrMetricTooOld,
	}
	testAddAndGet(t, 1, testData, expectedData, 1, expectedErrors, 0, false)
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
	testAddAndGet(t, 600, testData, expectedData, 9, nil, 2, false)
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
	testAddAndGet(t, 600, testData, expectedData, 9, nil, 3, false)
}

func TestROBAddAndGetDuplicateDisallowUpdate(t *testing.T) {
	testData := []schema.Point{
		{Ts: 1001, Val: 100},
		{Ts: 1001, Val: 200},
		{Ts: 1003, Val: 300},
		{Ts: 1003, Val: 0},
		{Ts: 1001, Val: 0},
	}
	expectedData := []schema.Point{
		{Ts: 1001, Val: 100},
		{Ts: 1003, Val: 300},
	}
	expectedErrors := []error{
		nil,
		errors.ErrMetricNewValueForTimestamp,
		nil,
		errors.ErrMetricNewValueForTimestamp,
		errors.ErrMetricNewValueForTimestamp,
	}
	testAddAndGet(t, 600, testData, expectedData, 2, expectedErrors, 0, false)
}

func TestROBAddAndGetDuplicateAllowUpdate(t *testing.T) {
	testData := []schema.Point{
		{Ts: 1001, Val: 100},
		{Ts: 1001, Val: 200},
		{Ts: 1003, Val: 300},
		{Ts: 1003, Val: 0},
		{Ts: 1001, Val: 0},
	}
	expectedData := []schema.Point{
		{Ts: 1001, Val: 0},
		{Ts: 1003, Val: 0},
	}
	expectedErrors := []error{
		nil,
		nil,
		nil,
		nil,
		nil,
	}
	testAddAndGet(t, 600, testData, expectedData, 5, expectedErrors, 0, true)
}

func TestROBOmitFlushIfNotEnoughData(t *testing.T) {
	b := NewReorderBuffer(9, 1, false)
	for i := uint32(1); i < 10; i++ {
		pt, _, _ := b.Add(i, float64(i*100))
		if pt.Ts != 0 {
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
	expectedErrors := []error{
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		errors.ErrMetricTooOld,
	}
	flushedData := testAddAndGet(t, 5, testData, expectedData, 8, expectedErrors, 2, false)
	if !reflect.DeepEqual(flushedData, expectedFlushedData) {
		t.Fatalf("Flushed data does not match expected flushed data:\n%+v\n%+v", flushedData, expectedFlushedData)
	}
}

func TestROBFlushSortedData(t *testing.T) {
	var results []schema.Point
	buf := NewReorderBuffer(600, 1, false)
	for i := 1100; i < 2100; i++ {
		pt, flushed, err := buf.Add(uint32(i), float64(i))
		if err != nil {
			t.Fatalf("Adding failed")
		} else if pt.Ts != 0 {
			results = append(results, pt)
			results = append(results, flushed...)
		}
	}

	for i := 0; i < 400; i++ {
		if results[i].Ts != uint32(i+1100) || results[i].Val != float64(i+1100) {
			t.Fatalf("Unexpected results %+v", results)
		}
	}
}

func TestROBFlushUnsortedData1(t *testing.T) {
	var results []schema.Point
	buf := NewReorderBuffer(3, 1, false)
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
		pt, flushed, err := buf.Add(p.Ts, p.Val)
		if err != nil {
			failedCount++
		} else if pt.Ts != 0 {
			results = append(results, pt)
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
	buf := NewReorderBuffer(600, 1, false)
	data := make([]schema.Point, 1000)
	for i := 0; i < 1000; i++ {
		data[i] = schema.Point{Ts: uint32(i + 1000), Val: float64(i + 1000)}
	}
	unsortedData := unsort(data, 10)
	for i := 0; i < len(data); i++ {
		pt, flushed, _ := buf.Add(unsortedData[i].Ts, unsortedData[i].Val)
		if pt.Ts != 0 {
			results = append(results, pt)
			results = append(results, flushed...)
		}
	}
	for i := 0; i < 400; i++ {
		if results[i].Ts != uint32(i+1000) || results[i].Val != float64(i+1000) {
			t.Fatalf("Unexpected results %+v", results)
		}
	}
}

func TestROBAvoidUnderflow(t *testing.T) {
	var results []schema.Point
	windowSize := 10
	dpWindows := 10
	numDps := dpWindows * windowSize
	buf := NewReorderBuffer(uint32(windowSize), 1, false)
	data := make([]schema.Point, numDps)
	for i := 0; i < numDps; i++ {
		data[i] = schema.Point{Ts: uint32(i + 1), Val: float64(i + 1000)}
	}

	// Add datapoints out of order in half window steps
	for i := 0; i < numDps; i += windowSize / 2 {
		minIndex := i
		maxIndex := minIndex + windowSize/2 - 1
		for i := maxIndex; i >= minIndex; i-- {
			pt, flushed, err := buf.Add(data[i].Ts, data[i].Val)
			if err != nil {
				t.Fatalf("Got unexpected error = %v", err)
			}
			if pt.Ts != 0 {
				results = append(results, pt)
				results = append(results, flushed...)
			}
		}
	}

	expectedLen := len(data) - windowSize
	if len(results) != expectedLen {
		t.Fatalf("Expected %d results, got %d", expectedLen, len(results))
	}

	for i := 0; i < len(results); i++ {
		if results[i].Ts != uint32(i+1) || results[i].Val != float64(i+1000) {
			t.Fatalf("Unexpected results starting at %d %+v", i, results)
		}
	}
}

func TestROBFlushAndIsEmpty(t *testing.T) {
	buf := NewReorderBuffer(10, 1, false)

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

func BenchmarkROB10AddDisallowUpdate(b *testing.B) {
	benchmarkROBAdd(b, 10, 0, false)
}

func BenchmarkROB120AddDisallowUpdate(b *testing.B) {
	benchmarkROBAdd(b, 120, 0, false)
}

func BenchmarkROB600AddDisallowUpdate(b *testing.B) {
	benchmarkROBAdd(b, 600, 0, false)
}

func BenchmarkROB10AddShuffled5DisallowUpdate(b *testing.B) {
	benchmarkROBAdd(b, 10, 5, false)
}

func BenchmarkROB120AddShuffled5DisallowUpdate(b *testing.B) {
	benchmarkROBAdd(b, 120, 5, false)
}

func BenchmarkROB600AddShuffled5DisallowUpdate(b *testing.B) {
	benchmarkROBAdd(b, 600, 5, false)
}

func BenchmarkROB10AddShuffled50DisallowUpdate(b *testing.B) {
	benchmarkROBAdd(b, 10, 50, false)
}

func BenchmarkROB120AddShuffled50DisallowUpdate(b *testing.B) {
	benchmarkROBAdd(b, 120, 50, false)
}

func BenchmarkROB600AddShuffled50DisallowUpdate(b *testing.B) {
	benchmarkROBAdd(b, 600, 50, false)
}

func BenchmarkROB10AddShuffled500DisallowUpdate(b *testing.B) {
	benchmarkROBAdd(b, 10, 500, false)
}

func BenchmarkROB120AddShuffled500DisallowUpdate(b *testing.B) {
	benchmarkROBAdd(b, 120, 500, false)
}

func BenchmarkROB600AddShuffled500DisallowUpdate(b *testing.B) {
	benchmarkROBAdd(b, 600, 500, false)
}

func BenchmarkROB10AddAllowUpdate(b *testing.B) {
	benchmarkROBAdd(b, 10, 0, true)
}

func BenchmarkROB120AddAllowUpdate(b *testing.B) {
	benchmarkROBAdd(b, 120, 0, true)
}

func BenchmarkROB600AddAllowUpdate(b *testing.B) {
	benchmarkROBAdd(b, 600, 0, true)
}

func BenchmarkROB10AddShuffled5AllowUpdate(b *testing.B) {
	benchmarkROBAdd(b, 10, 5, true)
}

func BenchmarkROB120AddShuffled5AllowUpdate(b *testing.B) {
	benchmarkROBAdd(b, 120, 5, true)
}

func BenchmarkROB600AddShuffled5AllowUpdate(b *testing.B) {
	benchmarkROBAdd(b, 600, 5, true)
}

func BenchmarkROB10AddShuffled50AllowUpdate(b *testing.B) {
	benchmarkROBAdd(b, 10, 50, true)
}

func BenchmarkROB120AddShuffled50AllowUpdate(b *testing.B) {
	benchmarkROBAdd(b, 120, 50, true)
}

func BenchmarkROB600AddShuffled50AllowUpdate(b *testing.B) {
	benchmarkROBAdd(b, 600, 50, true)
}

func BenchmarkROB10AddShuffled500AllowUpdate(b *testing.B) {
	benchmarkROBAdd(b, 10, 500, true)
}

func BenchmarkROB120AddShuffled500AllowUpdate(b *testing.B) {
	benchmarkROBAdd(b, 120, 500, true)
}

func BenchmarkROB600AddShuffled500AllowUpdate(b *testing.B) {
	benchmarkROBAdd(b, 600, 500, true)
}

func benchmarkROBAdd(b *testing.B, window, shufgroup int, allowUpdate bool) {
	data := NewInputData(b.N)
	if shufgroup > 1 {
		data = unsort(data, shufgroup)
	}

	rob := NewReorderBuffer(uint32(window), 1, allowUpdate)
	var out []schema.Point
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, out, _ = rob.Add(data[i].Ts, data[i].Val)
	}
	if len(out) > 1000 {
		panic("this clause should never fire. only exists for compiler not to optimize away the results")
	}
}

func NewInputData(num int) []schema.Point {
	ret := make([]schema.Point, num)
	for i := 0; i < num; i++ {
		ret[i] = schema.Point{
			Val: float64(i + 1),
			Ts:  uint32(i + 1),
		}
	}
	return ret
}
