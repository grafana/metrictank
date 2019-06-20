package main

import (
	"math"
	"testing"

	"github.com/grafana/metrictank/mdata/chunk"

	"github.com/kisielk/whisper-go/whisper"
	"github.com/raintank/schema"
)

func testIncResolution(t *testing.T, inData []whisper.Point, expectedResult map[schema.Method][]whisper.Point, method schema.Method, inRes, outRes, rawRes uint32) {
	t.Helper()
	outData := incResolution(inData, method, inRes, outRes, rawRes)

	if len(expectedResult) != len(outData) {
		t.Fatalf("Generated data is not as expected:\nExpected:\n%+v\nGot:\n%+v\n", expectedResult, outData)
	}

	for m, ep := range expectedResult {
		var p []whisper.Point
		var ok bool
		if p, ok = outData[m]; !ok {
			t.Fatalf("testIncResolution.\nExpected:\n%+v\nGot:\n%+v\n", expectedResult, outData)
		}
		if len(p) != len(outData[m]) {
			t.Fatalf("testIncResolution.\nExpected:\n%+v\nGot:\n%+v\n", expectedResult, outData)
		}
		for i := range p {
			if p[i] != ep[i] {
				t.Fatalf("Datapoint does not match expected data:\nExpected:\n%+v\nGot:\n%+v\n", expectedResult, outData)
			}
		}
	}
}

func TestIncResolutionUpToTime(t *testing.T) {
	inData := []whisper.Point{
		{10, 10},
		{20, 11},
	}

	expectedResult := map[schema.Method][]whisper.Point{
		schema.Sum: {
			{5, 50},
		},
		schema.Cnt: {
			{5, 5},
		},
	}
	*importUpTo = uint(5)
	testIncResolution(t, inData, expectedResult, fakeAvg, 10, 5, 1)
	*importUpTo = math.MaxUint32
}

func TestIncResolutionFakeAvgNonFactorResolutions(t *testing.T) {
	inData := []whisper.Point{
		{10, 10},
		{20, 11},
		{30, 12},
		{40, 13},
		{50, 14},
	}

	expectedResult := map[schema.Method][]whisper.Point{
		schema.Sum: {
			{3, 30},
			{6, 30},
			{9, 30},
			{12, 33},
			{15, 33},
			{18, 33},
			{21, 36},
			{24, 36},
			{27, 36},
			{30, 36},
			{33, 39},
			{36, 39},
			{39, 39},
			{42, 42},
			{45, 42},
			{48, 42},
		},
		schema.Cnt: {
			{3, 3},
			{6, 3},
			{9, 3},
			{12, 3},
			{15, 3},
			{18, 3},
			{21, 3},
			{24, 3},
			{27, 3},
			{30, 3},
			{33, 3},
			{36, 3},
			{39, 3},
			{42, 3},
			{45, 3},
			{48, 3},
		},
	}

	testIncResolution(t, inData, expectedResult, fakeAvg, 10, 3, 1)
}

func TestIncFakeAvgResolutionWithGaps(t *testing.T) {
	inData := []whisper.Point{
		{0, 0},
		{10, 10},
		{0, 0},
		{0, 0},
		{40, 13},
		{50, 14},
		{0, 0},
	}

	expectedResult := map[schema.Method][]whisper.Point{
		schema.Sum: {
			{5, 50},
			{10, 50},
			{35, 65},
			{40, 65},
			{45, 70},
			{50, 70},
		},
		schema.Cnt: {
			{5, 5},
			{10, 5},
			{35, 5},
			{40, 5},
			{45, 5},
			{50, 5},
		},
	}

	testIncResolution(t, inData, expectedResult, fakeAvg, 10, 5, 1)
}

func TestIncFakeAvgResolutionOutOfOrder(t *testing.T) {
	inData := []whisper.Point{
		{40, 13},
		{10, 10},
		{50, 14},
	}

	expectedResult := map[schema.Method][]whisper.Point{
		schema.Sum: {
			{5, 50},
			{10, 50},
			{35, 65},
			{40, 65},
			{45, 70},
			{50, 70},
		},
		schema.Cnt: {
			{5, 5},
			{10, 5},
			{35, 5},
			{40, 5},
			{45, 5},
			{50, 5},
		},
	}

	testIncResolution(t, inData, expectedResult, fakeAvg, 10, 5, 1)
}

func TestIncFakeAvgResolutionStrangeRawRes(t *testing.T) {
	inData := []whisper.Point{
		{30, 10},
		{60, 11},
		{90, 12},
	}

	aggFactor := float64(10) / float64(3)

	expectedResult := map[schema.Method][]whisper.Point{
		schema.Sum: {
			{10, 10 * aggFactor},
			{20, 10 * aggFactor},
			{30, 10 * aggFactor},
			{40, 11 * aggFactor},
			{50, 11 * aggFactor},
			{60, 11 * aggFactor},
			{70, 12 * aggFactor},
			{80, 12 * aggFactor},
			{90, 12 * aggFactor},
		},
		schema.Cnt: {
			{10, aggFactor},
			{20, aggFactor},
			{30, aggFactor},
			{40, aggFactor},
			{50, aggFactor},
			{60, aggFactor},
			{70, aggFactor},
			{80, aggFactor},
			{90, aggFactor},
		},
	}

	testIncResolution(t, inData, expectedResult, fakeAvg, 30, 10, 3)
}

func TestIncResolutionSimpleMax(t *testing.T) {
	inData := []whisper.Point{
		{10, 10},
		{20, 11},
	}

	expectedResult := map[schema.Method][]whisper.Point{
		schema.Max: {
			{5, 10},
			{10, 10},
			{15, 11},
			{20, 11},
		},
	}
	testIncResolution(t, inData, expectedResult, schema.Max, 10, 5, 1)
}

func TestIncResolutionSimpleLast(t *testing.T) {
	inData := []whisper.Point{
		{10, 10},
		{20, 11},
	}

	expectedResult := map[schema.Method][]whisper.Point{
		schema.Lst: {
			{5, 10},
			{10, 10},
			{15, 11},
			{20, 11},
		},
	}
	testIncResolution(t, inData, expectedResult, schema.Lst, 10, 5, 1)
}

func TestIncResolutionSimpleMin(t *testing.T) {
	inData := []whisper.Point{
		{10, 10},
		{20, 11},
	}

	expectedResult := map[schema.Method][]whisper.Point{
		schema.Min: {
			{5, 10},
			{10, 10},
			{15, 11},
			{20, 11},
		},
	}
	testIncResolution(t, inData, expectedResult, schema.Min, 10, 5, 1)
}

func TestIncResolutionSimpleAvg(t *testing.T) {
	inData := []whisper.Point{
		{10, 10},
		{20, 11},
	}

	expectedResult := map[schema.Method][]whisper.Point{
		schema.Avg: {
			{5, 10},
			{10, 10},
			{15, 11},
			{20, 11},
		},
	}
	testIncResolution(t, inData, expectedResult, schema.Avg, 10, 5, 1)
}

func TestIncResolutionSimpleFakeAvg(t *testing.T) {
	inData := []whisper.Point{
		{10, 10},
		{20, 11},
	}

	expectedResult := map[schema.Method][]whisper.Point{
		schema.Sum: {
			{5, 50},
			{10, 50},
			{15, 55},
			{20, 55},
		},
		schema.Cnt: {
			{5, 5},
			{10, 5},
			{15, 5},
			{20, 5},
		},
	}
	testIncResolution(t, inData, expectedResult, fakeAvg, 10, 5, 1)
}

func TestIncResolutionSimpleSum(t *testing.T) {
	inData := []whisper.Point{
		{10, 10},
		{20, 11},
	}

	expectedResult := map[schema.Method][]whisper.Point{
		schema.Sum: {
			{5, 5},
			{10, 5},
			{15, 5.5},
			{20, 5.5},
		},
		schema.Cnt: {
			{5, 5},
			{10, 5},
			{15, 5},
			{20, 5},
		},
	}
	testIncResolution(t, inData, expectedResult, schema.Sum, 10, 5, 1)
}

func TestIncResolutionNonFactorResolutions(t *testing.T) {
	inData := []whisper.Point{
		{10, 10},
		{20, 11},
		{30, 12},
		{40, 13},
		{50, 14},
	}

	expectedResult := map[schema.Method][]whisper.Point{
		schema.Max: {
			{3, 10},
			{6, 10},
			{9, 10},
			{12, 11},
			{15, 11},
			{18, 11},
			{21, 12},
			{24, 12},
			{27, 12},
			{30, 12},
			{33, 13},
			{36, 13},
			{39, 13},
			{42, 14},
			{45, 14},
			{48, 14},
		},
	}

	testIncResolution(t, inData, expectedResult, schema.Max, 10, 3, 1)
}

func TestIncResolutionWithGaps(t *testing.T) {
	inData := []whisper.Point{
		{0, 0},
		{10, 10},
		{0, 0},
		{0, 0},
		{40, 13},
		{50, 14},
		{0, 0},
	}

	expectedResult := map[schema.Method][]whisper.Point{
		schema.Max: {
			{5, 10},
			{10, 10},
			{35, 13},
			{40, 13},
			{45, 14},
			{50, 14},
		},
	}

	testIncResolution(t, inData, expectedResult, schema.Max, 10, 5, 1)
}

func TestIncResolutionOutOfOrder(t *testing.T) {
	inData := []whisper.Point{
		{40, 13},
		{10, 10},
		{50, 14},
	}

	expectedResult := map[schema.Method][]whisper.Point{
		schema.Max: {
			{5, 10},
			{10, 10},
			{35, 13},
			{40, 13},
			{45, 14},
			{50, 14},
		},
	}

	testIncResolution(t, inData, expectedResult, schema.Max, 10, 5, 1)
}

func testDecResolution(t *testing.T, inData []whisper.Point, expectedResult map[schema.Method][]whisper.Point, method schema.Method, inRes, outRes, rawRes uint32) {
	t.Helper()
	outData := decResolution(inData, method, inRes, outRes, rawRes)

	if len(expectedResult) != len(outData) {
		t.Fatalf("Generated data has different length (%d) than expected (%d):\n%+v\n%+v", len(expectedResult), len(outData), outData, expectedResult)
	}

	for m, ep := range expectedResult {
		var p []whisper.Point
		var ok bool
		if p, ok = outData[m]; !ok {
			t.Fatalf("Generated data is not as expected:\nExpected:\n%+v\nGot:\n%+v\n", expectedResult, outData)
		}
		if len(p) != len(outData[m]) {
			t.Fatalf("Generated data is not as expected:\nExpected:\n%+v\nGot:\n%+v\n", expectedResult, outData)
		}
		for i := range p {
			if p[i] != ep[i] {
				t.Fatalf("Datapoint does not match expected data:\nExpected:\n%+v\nGot:\n%+v\n", expectedResult, outData)
			}
		}
	}
}

func getSimpleInData() []whisper.Point {
	return []whisper.Point{
		{10, 10},
		{20, 11},
		{30, 12},
		{40, 13},
		{50, 14},
		{60, 15},
	}
}

func TestDecResolutionSimpleAvg(t *testing.T) {
	expectedResult := map[schema.Method][]whisper.Point{
		schema.Avg: {
			{30, 11},
			{60, 14},
		},
	}
	testDecResolution(t, getSimpleInData(), expectedResult, schema.Avg, 10, 30, 1)
}

func TestDecResolutionSimpleFakeAvg(t *testing.T) {
	expectedResult := map[schema.Method][]whisper.Point{
		schema.Sum: {
			{30, 330},
			{60, 420},
		},
		schema.Cnt: {
			{30, 30},
			{60, 30},
		},
	}
	testDecResolution(t, getSimpleInData(), expectedResult, fakeAvg, 10, 30, 1)
}

func TestDecResolutionSimpleSum(t *testing.T) {
	expectedResult := map[schema.Method][]whisper.Point{
		schema.Sum: {
			{30, 33},
			{60, 42},
		},
		schema.Cnt: {
			{30, 30},
			{60, 30},
		},
	}
	testDecResolution(t, getSimpleInData(), expectedResult, schema.Sum, 10, 30, 1)
}

func TestDecResolutionSimpleLast(t *testing.T) {
	expectedResult := map[schema.Method][]whisper.Point{
		schema.Lst: {
			{30, 12},
			{60, 15},
		},
	}
	testDecResolution(t, getSimpleInData(), expectedResult, schema.Lst, 10, 30, 1)
}

func TestDecResolutionSimpleMax(t *testing.T) {
	expectedResult := map[schema.Method][]whisper.Point{
		schema.Max: {
			{30, 12},
			{60, 15},
		},
	}
	testDecResolution(t, getSimpleInData(), expectedResult, schema.Max, 10, 30, 1)
}

func TestDecResolutionSimpleMin(t *testing.T) {
	expectedResult := map[schema.Method][]whisper.Point{
		schema.Min: {
			{30, 10},
			{60, 13},
		},
	}
	testDecResolution(t, getSimpleInData(), expectedResult, schema.Min, 10, 30, 1)
}

func TestDecResolutionUpToTime(t *testing.T) {
	inData := []whisper.Point{
		{10, 10},
		{20, 11},
		{30, 12},
		{40, 13},
		{50, 14},
		{60, 15},
	}

	expectedResult := map[schema.Method][]whisper.Point{
		schema.Sum: {
			{30, 33},
		},
		schema.Cnt: {
			{30, 6},
		},
	}
	*importUpTo = uint(40)
	testDecResolution(t, inData, expectedResult, schema.Sum, 10, 30, 5)
	*importUpTo = math.MaxUint32
}

func TestDecResolutionAvg(t *testing.T) {
	inData := []whisper.Point{
		{10, 10},
		{20, 11},
		{30, 12},
		{40, 13},
		{50, 14},
		{60, 15},
	}

	expectedResult := map[schema.Method][]whisper.Point{
		schema.Avg: {
			{30, 11},
			{60, 14},
		},
	}
	testDecResolution(t, inData, expectedResult, schema.Avg, 10, 30, 1)
}

func TestDecNonFactorResolutions(t *testing.T) {
	inData := []whisper.Point{
		{10, 10},
		{20, 11},
		{30, 12},
		{40, 13},
		{50, 14},
		{60, 15},
	}

	expectedResult := map[schema.Method][]whisper.Point{
		schema.Avg: {
			{15, 10},
			{30, 11.5},
			{45, 13},
			{60, 14.5},
		},
	}
	testDecResolution(t, inData, expectedResult, schema.Avg, 10, 15, 1)
}

func getGapData() []whisper.Point {
	return []whisper.Point{
		{0, 0},
		{10, 10},
		{0, 0},
		{0, 0},
		{40, 13},
		{50, 14},
		{0, 0},
		{70, 16},
	}
}

func TestDecResolutionWithGapsAvg(t *testing.T) {
	expectedResult := map[schema.Method][]whisper.Point{
		schema.Avg: {
			{20, 10},
			{40, 13},
			{60, 14},
		},
	}

	testDecResolution(t, getGapData(), expectedResult, schema.Avg, 10, 20, 1)
}

func TestDecResolutionWithGapsFakeAvg(t *testing.T) {
	expectedResult := map[schema.Method][]whisper.Point{
		schema.Sum: {
			{20, 100},
			{40, 130},
			{60, 140},
		},
		schema.Cnt: {
			{20, 10},
			{40, 10},
			{60, 10},
		},
	}

	testDecResolution(t, getGapData(), expectedResult, fakeAvg, 10, 20, 1)
}

func TestDecResolutionWithGapsSum(t *testing.T) {
	expectedResult := map[schema.Method][]whisper.Point{
		schema.Sum: {
			{20, 10},
			{40, 13},
			{60, 14},
		},
		schema.Cnt: {
			{20, 10},
			{40, 10},
			{60, 10},
		},
	}

	testDecResolution(t, getGapData(), expectedResult, schema.Sum, 10, 20, 1)
}

func TestDecResolutionOutOfOrder(t *testing.T) {
	inData := []whisper.Point{
		{20, 11},
		{50, 15},
		{30, 12},
		{10, 10},
		{60, 16},
		{40, 14},
	}

	expectedResult := map[schema.Method][]whisper.Point{
		schema.Avg: {
			{30, 11},
			{60, 15},
		},
	}
	testDecResolution(t, inData, expectedResult, schema.Avg, 10, 30, 1)
}

func TestDecFakeAvgNonFactorResolutions(t *testing.T) {
	inData := []whisper.Point{
		{10, 10},
		{20, 11},
		{30, 12},
		{40, 13},
		{50, 14},
		{60, 15},
	}

	expectedResult := map[schema.Method][]whisper.Point{
		schema.Sum: {
			{15, 10},
			{30, 23},
			{45, 13},
			{60, 29},
		},
		schema.Cnt: {
			{15, 10},
			{30, 20},
			{45, 10},
			{60, 20},
		},
	}
	testDecResolution(t, inData, expectedResult, schema.Sum, 10, 15, 1)
}

func TestDecResolutionFakeAvgOutOfOrder(t *testing.T) {
	inData := []whisper.Point{
		{20, 11},
		{50, 15},
		{30, 12},
		{10, 10},
		{60, 16},
		{40, 14},
	}

	expectedResult := map[schema.Method][]whisper.Point{
		schema.Sum: {
			{30, 33},
			{60, 45},
		},
		schema.Cnt: {
			{30, 30},
			{60, 30},
		},
	}
	testDecResolution(t, inData, expectedResult, schema.Sum, 10, 30, 1)
}

func generatePoints(ts, interval uint32, value float64, offset, count int, inc func(float64) float64) []whisper.Point {
	res := make([]whisper.Point, count)
	for i := 0; i < count; i++ {
		res[(i+offset)%count] = whisper.Point{
			Timestamp: ts,
			Value:     value,
		}
		ts += interval
		value = inc(value)
	}
	return res
}

func TestEncodedChunksFromPointsWithoutUnfinished(t *testing.T) {
	// the actual data in these points doesn't matter, we just want to be sure
	// that the chunks resulting from these points include the same data
	points := generatePoints(25200, 10, 10, 0, 8640, func(i float64) float64 { return i + 1 })
	expectedCount := 8640 - (2520 % 2160) // count minus what would end up in an unfinished chunk

	*writeUnfinishedChunks = false
	chunks := encodedChunksFromPoints(points, 10, 21600)

	if len(chunks) != 4 {
		t.Fatalf("Expected to get 4 chunks, but got %d", len(chunks))
	}

	i := 0
	for _, c := range chunks {
		iterGen, err := chunk.NewIterGen(c.Series.T0, 10, c.Encode(21600))
		if err != nil {
			t.Fatalf("Error getting iterator: %s", err)
		}

		iter, err := iterGen.Get()
		if err != nil {
			t.Fatalf("Error getting iterator: %s", err)
		}

		for iter.Next() {
			ts, val := iter.Values()
			if points[i].Timestamp != ts || points[i].Value != val {
				t.Fatalf("Unexpected value at index %d:\nExpected: %d:%f\nGot: %d:%f\n", i, ts, val, points[i].Timestamp, points[i].Value)
			}
			i++
		}
	}
	if i != expectedCount {
		t.Fatalf("Unexpected number of datapoints in chunks:\nExpected: %d\nGot: %d\n", expectedCount, i)
	}
}

func TestEncodedChunksFromPointsWithUnfinished(t *testing.T) {
	points := generatePoints(25200, 10, 10, 0, 8640, func(i float64) float64 { return i + 1 })
	expectedCount := 8640 // count including unfinished chunks

	*writeUnfinishedChunks = true
	chunks := encodedChunksFromPoints(points, 10, 21600)

	if len(chunks) != 5 {
		t.Fatalf("Expected to get 5 chunks, but got %d", len(chunks))
	}

	i := 0
	for _, c := range chunks {
		iterGen, err := chunk.NewIterGen(c.Series.T0, 10, c.Encode(21600))
		if err != nil {
			t.Fatalf("Error getting iterator: %s", err)
		}

		iter, err := iterGen.Get()
		if err != nil {
			t.Fatalf("Error getting iterator: %s", err)
		}

		for iter.Next() {
			ts, val := iter.Values()
			if points[i].Timestamp != ts || points[i].Value != val {
				t.Fatalf("Unexpected value at index %d:\nExpected: %d:%f\nGot: %d:%f\n", i, ts, val, points[i].Timestamp, points[i].Value)
			}
			i++
		}
	}
	if i != expectedCount {
		t.Fatalf("Unexpected number of datapoints in chunks:\nExpected: %d\nGot: %d\n", expectedCount, i)
	}
}

func verifyPointMaps(t *testing.T, points map[schema.Method][]whisper.Point, expected map[schema.Method][]whisper.Point) {
	t.Helper()
	for meth, ep := range expected {
		if _, ok := points[meth]; !ok {
			t.Fatalf("Missing method %s in result", meth)
		}
		if len(ep) != len(points[meth]) {
			t.Fatalf("Unexpected data returned in %s:\nGot:\n%+v\nExpected:\n%+v", meth, points[meth], expected[meth])
		}
		for i, p := range ep {
			if points[meth][i] != p {
				t.Fatalf("Unexpected data returned in %s:\nGot:\n%+v\nExpected:\n%+v", meth, points[meth], expected[meth])
			}
		}
	}
}

func TestPointsConversionSum1(t *testing.T) {
	c := conversion{
		archives: []whisper.ArchiveInfo{
			{SecondsPerPoint: 1, Points: 2},
			{SecondsPerPoint: 2, Points: 2},
			{SecondsPerPoint: 4, Points: 2},
		},
		points: map[int][]whisper.Point{
			0: {
				{1503384108, 100},
				{1503384109, 100},
			},
			1: {
				{1503384106, 200},
				{1503384108, 200},
			},
			2: {
				{1503384108, 200},
				{1503384104, 400},
			},
		},
		method: schema.Sum,
	}

	expectedPoints1 := map[schema.Method][]whisper.Point{
		schema.Sum: {
			{1503384102, 100},
			{1503384103, 100},
			{1503384104, 100},
			{1503384105, 100},
			{1503384106, 100},
			{1503384107, 100},
			{1503384108, 100},
			{1503384109, 100},
		},
	}
	expectedPoints2 := map[schema.Method][]whisper.Point{
		schema.Sum: {
			{1503384102, 200},
			{1503384104, 200},
			{1503384106, 200},
			{1503384108, 200},
		},
	}
	expectedPoints3 := map[schema.Method][]whisper.Point{
		schema.Sum: {
			{1503384104, 400},
			{1503384108, 200},
		},
	}

	points1 := c.getPoints(0, 1, 8)
	points2 := c.getPoints(0, 2, 4)
	points3 := c.getPoints(0, 4, 2)

	verifyPointMaps(t, points1, expectedPoints1)
	verifyPointMaps(t, points2, expectedPoints2)
	verifyPointMaps(t, points3, expectedPoints3)
}

func TestPointsConversionLast1(t *testing.T) {
	c := conversion{
		archives: []whisper.ArchiveInfo{
			{SecondsPerPoint: 1, Points: 2},
			{SecondsPerPoint: 2, Points: 2},
			{SecondsPerPoint: 4, Points: 2},
		},
		points: map[int][]whisper.Point{
			0: {
				{1503475281, 7},
				{1503475282, 8},
			},
			1: {
				{1503475282, 8},
				{1503475280, 7},
			},
			2: {
				{1503475280, 8},
				{1503475276, 5},
			},
		},
		method: schema.Lst,
	}

	expectedPoints1 := map[schema.Method][]whisper.Point{
		schema.Lst: {
			{1503475275, 5},
			{1503475276, 5},
			{1503475277, 8},
			{1503475278, 8},
			{1503475279, 7},
			{1503475280, 7},
			{1503475281, 7},
			{1503475282, 8},
		},
	}
	expectedPoints2 := map[schema.Method][]whisper.Point{
		schema.Lst: {
			{1503475276, 5},
			{1503475278, 8},
			{1503475280, 7},
			{1503475282, 8},
		},
	}
	expectedPoints3 := map[schema.Method][]whisper.Point{
		schema.Lst: {
			{1503475276, 5},
			{1503475280, 8},
		},
	}

	points1 := c.getPoints(0, 1, 8)
	points2 := c.getPoints(0, 2, 4)
	points3 := c.getPoints(0, 4, 2)

	verifyPointMaps(t, points1, expectedPoints1)
	verifyPointMaps(t, points2, expectedPoints2)
	verifyPointMaps(t, points3, expectedPoints3)
}

func TestPointsConversionSum2(t *testing.T) {
	c := conversion{
		archives: []whisper.ArchiveInfo{
			{SecondsPerPoint: 1, Points: 8},
			{SecondsPerPoint: 2, Points: 8},
			{SecondsPerPoint: 4, Points: 8},
		},
		points: map[int][]whisper.Point{
			0: {
				{1503331540, 100},
				{1503331533, 100},
				{1503331534, 100},
				{1503331535, 100},
				{1503331536, 100},
				{1503331537, 100},
				{1503331538, 100},
				{1503331539, 100},
			},
			1: {
				{1503331540, 100},
				{1503331526, 200},
				{1503331528, 200},
				{1503331530, 200},
				{1503331532, 200},
				{1503331534, 200},
				{1503331536, 200},
				{1503331538, 200},
			},
			2: {
				{1503331540, 100},
				{1503331512, 400},
				{1503331516, 400},
				{1503331520, 400},
				{1503331524, 400},
				{1503331528, 400},
				{1503331532, 400},
				{1503331536, 400},
			},
		},
		method: schema.Sum,
	}

	expectedPoints1 := map[schema.Method][]whisper.Point{
		schema.Sum: {
			{1503331509, 100},
			{1503331510, 100},
			{1503331511, 100},
			{1503331512, 100},
			{1503331513, 100},
			{1503331514, 100},
			{1503331515, 100},
			{1503331516, 100},
			{1503331517, 100},
			{1503331518, 100},
			{1503331519, 100},
			{1503331520, 100},
			{1503331521, 100},
			{1503331522, 100},
			{1503331523, 100},
			{1503331524, 100},
			{1503331525, 100},
			{1503331526, 100},
			{1503331527, 100},
			{1503331528, 100},
			{1503331529, 100},
			{1503331530, 100},
			{1503331531, 100},
			{1503331532, 100},
			{1503331533, 100},
			{1503331534, 100},
			{1503331535, 100},
			{1503331536, 100},
			{1503331537, 100},
			{1503331538, 100},
			{1503331539, 100},
			{1503331540, 100},
		},
	}

	expectedPoints2 := map[schema.Method][]whisper.Point{
		schema.Sum: {
			{1503331510, 200},
			{1503331512, 200},
			{1503331514, 200},
			{1503331516, 200},
			{1503331518, 200},
			{1503331520, 200},
			{1503331522, 200},
			{1503331524, 200},
			{1503331526, 200},
			{1503331528, 200},
			{1503331530, 200},
			{1503331532, 200},
			{1503331534, 200},
			{1503331536, 200},
			{1503331538, 200},
			{1503331540, 100},
		},
	}

	expectedPoints3 := map[schema.Method][]whisper.Point{
		schema.Sum: {
			{1503331512, 400},
			{1503331516, 400},
			{1503331520, 400},
			{1503331524, 400},
			{1503331528, 400},
			{1503331532, 400},
			{1503331536, 400},
			{1503331540, 100},
		},
	}

	points1 := c.getPoints(0, 1, 32)
	points2 := c.getPoints(1, 2, 16)
	points3 := c.getPoints(2, 4, 8)

	verifyPointMaps(t, points1, expectedPoints1)
	verifyPointMaps(t, points2, expectedPoints2)
	verifyPointMaps(t, points3, expectedPoints3)
}

func TestPointsConversionAvg1(t *testing.T) {
	c := conversion{
		archives: []whisper.ArchiveInfo{
			{SecondsPerPoint: 1, Points: 2},
			{SecondsPerPoint: 2, Points: 2},
			{SecondsPerPoint: 4, Points: 2},
		},
		points: map[int][]whisper.Point{
			0: {
				{1503407725, 7},
				{1503407726, 8},
			},
			1: {
				{1503407726, 8},
				{1503407724, 6.5},
			},
			2: {
				{1503407724, 7.25},
				{1503407720, 3.5},
			},
		},
		method: schema.Avg,
	}

	expectedPoints1_0 := map[schema.Method][]whisper.Point{
		schema.Avg: {
			{1503407719, 3.5},
			{1503407720, 3.5},
			{1503407721, 7.25},
			{1503407722, 7.25},
			{1503407723, 6.5},
			{1503407724, 6.5},
			{1503407725, 7},
			{1503407726, 8},
		},
	}
	expectedPoints2_0 := map[schema.Method][]whisper.Point{
		schema.Avg: {
			{1503407720, 3.5},
			{1503407722, 7.25},
			{1503407724, 6.5},
			{1503407726, 8},
		},
	}
	expectedPoints3_0 := map[schema.Method][]whisper.Point{
		schema.Avg: {
			{1503407720, 3.5},
			{1503407724, 7.25},
		},
	}

	expectedPoints1_1 := map[schema.Method][]whisper.Point{
		schema.Sum: {
			{1503407719, 3.5},
			{1503407720, 3.5},
			{1503407721, 7.25},
			{1503407722, 7.25},
			{1503407723, 6.5},
			{1503407724, 6.5},
			{1503407725, 7},
			{1503407726, 8},
		},
		schema.Cnt: {
			{1503407719, 1},
			{1503407720, 1},
			{1503407721, 1},
			{1503407722, 1},
			{1503407723, 1},
			{1503407724, 1},
			{1503407725, 1},
			{1503407726, 1},
		},
	}
	expectedPoints2_1 := map[schema.Method][]whisper.Point{
		schema.Sum: {
			{1503407720, 3.5 * 2},
			{1503407722, 7.25 * 2},
			{1503407724, 6.5 * 2},
			{1503407726, 8 * 2},
		},
		schema.Cnt: {
			{1503407720, 2},
			{1503407722, 2},
			{1503407724, 2},
			{1503407726, 2},
		},
	}
	expectedPoints3_1 := map[schema.Method][]whisper.Point{
		schema.Sum: {
			{1503407720, 3.5 * 4},
			{1503407724, 7.25 * 4},
		},
		schema.Cnt: {
			{1503407720, 4},
			{1503407724, 4},
		},
	}

	expectedPoints1_2 := map[schema.Method][]whisper.Point{
		schema.Sum: {
			{1503407717, 3.5},
			{1503407718, 3.5},
			{1503407719, 3.5},
			{1503407720, 3.5},
			{1503407721, 7.25},
			{1503407722, 7.25},
			{1503407723, 6.5},
		},
		schema.Cnt: {
			{1503407717, 1},
			{1503407718, 1},
			{1503407719, 1},
			{1503407720, 1},
			{1503407721, 1},
			{1503407722, 1},
			{1503407723, 1},
		},
	}
	expectedPoints2_2 := map[schema.Method][]whisper.Point{
		schema.Sum: {
			{1503407718, 3.5 * 2},
			{1503407720, 3.5 * 2},
			{1503407722, 7.25 * 2},
		},
		schema.Cnt: {
			{1503407718, 2},
			{1503407720, 2},
			{1503407722, 2},
		},
	}
	expectedPoints3_2 := map[schema.Method][]whisper.Point{
		schema.Sum: {
			{1503407720, 3.5 * 4},
		},
		schema.Cnt: {
			{1503407720, 1 * 4},
		},
	}

	points1_0 := c.getPoints(0, 1, 8)
	points2_0 := c.getPoints(0, 2, 4)
	points3_0 := c.getPoints(0, 4, 2)

	points1_1 := c.getPoints(1, 1, 8)
	points2_1 := c.getPoints(1, 2, 4)
	points3_1 := c.getPoints(1, 4, 100)

	*importUpTo = uint(1503407723)
	points1_2 := c.getPoints(1, 1, 8)
	points2_2 := c.getPoints(1, 2, 100)
	points3_2 := c.getPoints(1, 4, 8)
	*importUpTo = math.MaxUint32

	verifyPointMaps(t, points1_0, expectedPoints1_0)
	verifyPointMaps(t, points2_0, expectedPoints2_0)
	verifyPointMaps(t, points3_0, expectedPoints3_0)

	verifyPointMaps(t, points1_1, expectedPoints1_1)
	verifyPointMaps(t, points2_1, expectedPoints2_1)
	verifyPointMaps(t, points3_1, expectedPoints3_1)

	verifyPointMaps(t, points1_2, expectedPoints1_2)
	verifyPointMaps(t, points2_2, expectedPoints2_2)
	verifyPointMaps(t, points3_2, expectedPoints3_2)
}

func TestPointsConversionAvg2(t *testing.T) {
	c := conversion{
		archives: []whisper.ArchiveInfo{
			{SecondsPerPoint: 1, Points: 3},
			{SecondsPerPoint: 3, Points: 3},
			{SecondsPerPoint: 9, Points: 3},
		},
		points: map[int][]whisper.Point{
			0: {
				{1503406145, 25},
				{1503406146, 26},
				{1503406147, 27},
			},
			1: {
				{1503406140, 21},
				{1503406143, 24},
				{1503406146, 26.5},
			},
			2: {
				{1503406125, 9},
				{1503406134, 18},
				{1503406143, 25.25},
			},
		},
		method: schema.Avg,
	}

	expectedPoints1_0 := map[schema.Method][]whisper.Point{
		schema.Avg: {
			{1503406121, 9},
			{1503406122, 9},
			{1503406123, 9},
			{1503406124, 9},
			{1503406125, 9},
			{1503406126, 18},
			{1503406127, 18},
			{1503406128, 18},
			{1503406129, 18},
			{1503406130, 18},
			{1503406131, 18},
			{1503406132, 18},
			{1503406133, 18},
			{1503406134, 18},
			{1503406135, 25.25},
			{1503406136, 25.25},
			{1503406137, 25.25},
			{1503406138, 21},
			{1503406139, 21},
			{1503406140, 21},
			{1503406141, 24},
			{1503406142, 24},
			{1503406143, 24},
			{1503406144, 26.5},
			{1503406145, 25},
			{1503406146, 26},
			{1503406147, 27},
		},
	}

	expectedPoints1_1 := map[schema.Method][]whisper.Point{
		schema.Sum: {
			{1503406121, 9},
			{1503406122, 9},
			{1503406123, 9},
			{1503406124, 9},
			{1503406125, 9},
			{1503406126, 18},
			{1503406127, 18},
			{1503406128, 18},
			{1503406129, 18},
			{1503406130, 18},
			{1503406131, 18},
			{1503406132, 18},
			{1503406133, 18},
			{1503406134, 18},
			{1503406135, 25.25},
			{1503406136, 25.25},
			{1503406137, 25.25},
			{1503406138, 21},
			{1503406139, 21},
			{1503406140, 21},
			{1503406141, 24},
			{1503406142, 24},
			{1503406143, 24},
			{1503406144, 26.5},
			{1503406145, 25},
			{1503406146, 26},
			{1503406147, 27},
		},
		schema.Cnt: {
			{1503406121, 1},
			{1503406122, 1},
			{1503406123, 1},
			{1503406124, 1},
			{1503406125, 1},
			{1503406126, 1},
			{1503406127, 1},
			{1503406128, 1},
			{1503406129, 1},
			{1503406130, 1},
			{1503406131, 1},
			{1503406132, 1},
			{1503406133, 1},
			{1503406134, 1},
			{1503406135, 1},
			{1503406136, 1},
			{1503406137, 1},
			{1503406138, 1},
			{1503406139, 1},
			{1503406140, 1},
			{1503406141, 1},
			{1503406142, 1},
			{1503406143, 1},
			{1503406144, 1},
			{1503406145, 1},
			{1503406146, 1},
			{1503406147, 1},
		},
	}

	expectedPoints2_0 := map[schema.Method][]whisper.Point{
		schema.Avg: {
			{1503406119, 9},
			{1503406122, 9},
			{1503406125, 9},
			{1503406128, 18},
			{1503406131, 18},
			{1503406134, 18},
			{1503406137, 25.25},
			{1503406140, 21},
			{1503406143, 24},
			{1503406146, 26.5},
		},
	}

	expectedPoints2_1 := map[schema.Method][]whisper.Point{
		schema.Sum: {
			{1503406122, 9 * 3},
			{1503406125, 9 * 3},
			{1503406128, 18 * 3},
			{1503406131, 18 * 3},
			{1503406134, 18 * 3},
			{1503406137, 25.25 * 3},
			{1503406140, 21 * 3},
			{1503406143, 24 * 3},
			{1503406146, 26.5 * 3},
		},
		schema.Cnt: {
			{1503406122, 3},
			{1503406125, 3},
			{1503406128, 3},
			{1503406131, 3},
			{1503406134, 3},
			{1503406137, 3},
			{1503406140, 3},
			{1503406143, 3},
			{1503406146, 3},
		},
	}

	expectedPoints3_0 := map[schema.Method][]whisper.Point{
		schema.Avg: {
			{1503406125, 9},
			{1503406134, 18},
			{1503406143, 25.25},
		},
	}

	expectedPoints3_1 := map[schema.Method][]whisper.Point{
		schema.Sum: {
			{1503406125, 9 * 9},
			{1503406134, 18 * 9},
			{1503406143, 25.25 * 9},
		},
		schema.Cnt: {
			{1503406125, 9},
			{1503406134, 9},
			{1503406143, 9},
		},
	}

	points1_0 := c.getPoints(0, 1, 27)
	points2_0 := c.getPoints(0, 3, 100)
	points3_0 := c.getPoints(0, 9, 100)

	points1_1 := c.getPoints(1, 1, 27)
	points2_1 := c.getPoints(1, 3, 9)
	points3_1 := c.getPoints(1, 9, 3)

	verifyPointMaps(t, points1_0, expectedPoints1_0)
	verifyPointMaps(t, points2_0, expectedPoints2_0)
	verifyPointMaps(t, points3_0, expectedPoints3_0)

	verifyPointMaps(t, points1_1, expectedPoints1_1)
	verifyPointMaps(t, points2_1, expectedPoints2_1)
	verifyPointMaps(t, points3_1, expectedPoints3_1)
}
