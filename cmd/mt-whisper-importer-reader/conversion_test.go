package main

import (
	"testing"

	"github.com/kisielk/whisper-go/whisper"
)

func testIncResolutionFakeAvg(t *testing.T, inData []whisper.Point, expectedResult map[string][]whisper.Point, inRes, outRes uint32) {
	outData := incResolutionFakeAvg(inData, inRes, outRes)

	if len(expectedResult) != len(outData) {
		t.Fatalf("Generated data is not as expected:\n%+v\n%+v", outData, expectedResult)
	}

	for m, ep := range expectedResult {
		var p []whisper.Point
		var ok bool
		if p, ok = outData[m]; !ok {
			t.Fatalf("Generated data is not as expected:\n%+v\n%+v", outData, expectedResult)
		}
		if len(p) != len(outData[m]) {
			t.Fatalf("Generated data is not as expected:\n%+v\n%+v", outData, expectedResult)
		}
		for i, _ := range p {
			if p[i] != ep[i] {
				t.Fatalf("Datapoint does not match expected data:\n%+v\n%+v", outData, expectedResult)
			}
		}
	}
}

func TestIncResolutionFakeAvgSimple(t *testing.T) {
	inData := []whisper.Point{
		{10, 10},
		{20, 11},
	}

	expectedResult := map[string][]whisper.Point{
		"sum": {
			{5, 5},
			{10, 5},
			{15, 5.5},
			{20, 5.5},
		},
		"cnt": {
			{5, 0.5},
			{10, 0.5},
			{15, 0.5},
			{20, 0.5},
		},
	}
	testIncResolutionFakeAvg(t, inData, expectedResult, 10, 5)
}

func TestIncResolutionFakeAvgNonFactorResolutions(t *testing.T) {
	inData := []whisper.Point{
		{10, 10},
		{20, 11},
		{30, 12},
		{40, 13},
		{50, 14},
	}

	expectedResult := map[string][]whisper.Point{
		"sum": {
			{3, float64(10) / 3},
			{6, float64(10) / 3},
			{9, float64(10) / 3},
			{12, float64(11) / 3},
			{15, float64(11) / 3},
			{18, float64(11) / 3},
			{21, float64(12) / 4},
			{24, float64(12) / 4},
			{27, float64(12) / 4},
			{30, float64(12) / 4},
			{33, float64(13) / 3},
			{36, float64(13) / 3},
			{39, float64(13) / 3},
			{42, float64(14) / 3},
			{45, float64(14) / 3},
			{48, float64(14) / 3},
		},
		"cnt": {
			{3, float64(1) / 3},
			{6, float64(1) / 3},
			{9, float64(1) / 3},
			{12, float64(1) / 3},
			{15, float64(1) / 3},
			{18, float64(1) / 3},
			{21, float64(1) / 4},
			{24, float64(1) / 4},
			{27, float64(1) / 4},
			{30, float64(1) / 4},
			{33, float64(1) / 3},
			{36, float64(1) / 3},
			{39, float64(1) / 3},
			{42, float64(1) / 3},
			{45, float64(1) / 3},
			{48, float64(1) / 3},
		},
	}

	testIncResolutionFakeAvg(t, inData, expectedResult, 10, 3)
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

	expectedResult := map[string][]whisper.Point{
		"sum": {
			{5, 5},
			{10, 5},
			{35, 6.5},
			{40, 6.5},
			{45, 7},
			{50, 7},
		},
		"cnt": {
			{5, 0.5},
			{10, 0.5},
			{35, 0.5},
			{40, 0.5},
			{45, 0.5},
			{50, 0.5},
		},
	}

	testIncResolutionFakeAvg(t, inData, expectedResult, 10, 5)
}

func TestIncFakeAvgResolutionOutOfOrder(t *testing.T) {
	inData := []whisper.Point{
		{40, 13},
		{10, 10},
		{50, 14},
	}

	expectedResult := map[string][]whisper.Point{
		"sum": {
			{5, 5},
			{10, 5},
			{35, 6.5},
			{40, 6.5},
			{45, 7},
			{50, 7},
		},
		"cnt": {
			{5, 0.5},
			{10, 0.5},
			{35, 0.5},
			{40, 0.5},
			{45, 0.5},
			{50, 0.5},
		},
	}

	testIncResolutionFakeAvg(t, inData, expectedResult, 10, 5)
}

func testIncResolution(t *testing.T, inData, expectedResult []whisper.Point, method string, inRes, outRes uint32) {
	outData := incResolution(inData, method, inRes, outRes)

	if len(expectedResult) != len(outData) {
		t.Fatalf("Generated data has different length (%d) than expected (%d):\n%+v\n%+v", len(outData), len(expectedResult), outData, expectedResult)
	}

	for i := 0; i < len(expectedResult); i++ {
		if outData[i] != expectedResult[i] {
			t.Fatalf("Datapoint does not match expected data:\n%+v\n%+v", outData, expectedResult)
		}
	}
}

func TestIncResolutionSimple(t *testing.T) {
	inData := []whisper.Point{
		{10, 10},
		{20, 11},
	}

	expectedResult := []whisper.Point{
		{5, 10},
		{10, 10},
		{15, 11},
		{20, 11},
	}
	testIncResolution(t, inData, expectedResult, "max", 10, 5)
}

func TestIncResolutionNonFactorResolutions(t *testing.T) {
	inData := []whisper.Point{
		{10, 10},
		{20, 11},
		{30, 12},
		{40, 13},
		{50, 14},
	}

	expectedResult := []whisper.Point{
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
	}

	testIncResolution(t, inData, expectedResult, "max", 10, 3)
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

	expectedResult := []whisper.Point{
		{5, 10},
		{10, 10},
		{35, 13},
		{40, 13},
		{45, 14},
		{50, 14},
	}

	testIncResolution(t, inData, expectedResult, "max", 10, 5)
}

func TestIncResolutionOutOfOrder(t *testing.T) {
	inData := []whisper.Point{
		{40, 13},
		{10, 10},
		{50, 14},
	}

	expectedResult := []whisper.Point{
		{5, 10},
		{10, 10},
		{35, 13},
		{40, 13},
		{45, 14},
		{50, 14},
	}

	testIncResolution(t, inData, expectedResult, "max", 10, 5)
}

func testDecResolution(t *testing.T, inData, expectedResult []whisper.Point, aggMethod string, inRes, outRes uint32) {
	outData := decResolution(inData, aggMethod, inRes, outRes)

	if len(expectedResult) != len(outData) {
		t.Fatalf("Generated data has different length (%d) than expected (%d):\n%+v\n%+v", len(expectedResult), len(outData), outData, expectedResult)
	}

	for i := 0; i < len(expectedResult); i++ {
		if outData[i] != expectedResult[i] {
			t.Fatalf("Datapoint does not match expected data:\n%+v\n%+v", outData, expectedResult)
		}
	}
}

func TestDecResolutionSimple(t *testing.T) {
	inData := []whisper.Point{
		{10, 10},
		{20, 11},
		{30, 12},
		{40, 14},
		{50, 15},
		{60, 16},
	}

	expectedResult := []whisper.Point{
		{30, 33},
		{60, 45},
	}
	testDecResolution(t, inData, expectedResult, "sum", 10, 30)
}

func TestDecResolutionAvg(t *testing.T) {
	inData := []whisper.Point{
		{10, 10},
		{20, 11},
		{30, 12},
		{40, 14},
		{50, 15},
		{60, 16},
	}

	expectedResult := []whisper.Point{
		{30, 11},
		{60, 15},
	}
	testDecResolution(t, inData, expectedResult, "avg", 10, 30)
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

	expectedResult := []whisper.Point{
		{15, 10},
		{30, 11.5},
		{45, 13},
		{60, 14.5},
	}
	testDecResolution(t, inData, expectedResult, "avg", 10, 15)
}

func TestDecResolutionWithGaps(t *testing.T) {
	inData := []whisper.Point{
		{0, 0},
		{10, 10},
		{0, 0},
		{0, 0},
		{40, 13},
		{50, 14},
		{0, 0},
		{70, 16},
	}

	expectedResult := []whisper.Point{
		{20, 10},
		{40, 13},
		{60, 14},
	}

	testDecResolution(t, inData, expectedResult, "avg", 10, 20)
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

	expectedResult := []whisper.Point{
		{30, 11},
		{60, 15},
	}
	testDecResolution(t, inData, expectedResult, "avg", 10, 30)
}

func testDecResolutionFakeAvg(t *testing.T, inData []whisper.Point, expectedResult map[string][]whisper.Point, inRes, outRes uint32) {
	outData := decResolutionFakeAvg(inData, inRes, outRes)

	if len(expectedResult) != len(outData) {
		t.Fatalf("Generated data has different length (%d) than expected (%d):\n%+v\n%+v", len(expectedResult), len(outData), outData, expectedResult)
	}

	for m, ep := range expectedResult {
		var p []whisper.Point
		var ok bool
		if p, ok = outData[m]; !ok {
			t.Fatalf("Generated data is not as expected:\n%+v\n%+v", outData, expectedResult)
		}
		if len(p) != len(outData[m]) {
			t.Fatalf("Generated data is not as expected:\n%+v\n%+v", outData, expectedResult)
		}
		for i, _ := range p {
			if p[i] != ep[i] {
				t.Fatalf("Datapoint does not match expected data:\n%+v\n%+v", outData, expectedResult)
			}
		}
	}
}

func TestDecResolutionFakeAvgSimple(t *testing.T) {
	inData := []whisper.Point{
		{10, 10},
		{20, 11},
		{30, 12},
		{40, 14},
		{50, 15},
		{60, 16},
	}

	expectedResult := map[string][]whisper.Point{
		"sum": {
			{30, 33},
			{60, 45},
		},
		"cnt": {
			{30, 3},
			{60, 3},
		},
	}
	testDecResolutionFakeAvg(t, inData, expectedResult, 10, 30)
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

	expectedResult := map[string][]whisper.Point{
		"sum": {
			{15, 10},
			{30, 23},
			{45, 13},
			{60, 29},
		},
		"cnt": {
			{15, 1},
			{30, 2},
			{45, 1},
			{60, 2},
		},
	}
	testDecResolutionFakeAvg(t, inData, expectedResult, 10, 15)
}

func TestDecFakeAvgResolutionWithGaps(t *testing.T) {
	inData := []whisper.Point{
		{0, 0},
		{10, 10},
		{0, 0},
		{0, 0},
		{40, 13},
		{50, 14},
		{0, 0},
		{70, 16},
	}

	expectedResult := map[string][]whisper.Point{
		"sum": {
			{20, 10},
			{40, 13},
			{60, 14},
		},
		"cnt": {
			{20, 1},
			{40, 1},
			{60, 1},
		},
	}

	testDecResolutionFakeAvg(t, inData, expectedResult, 10, 20)
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

	expectedResult := map[string][]whisper.Point{
		"sum": {
			{30, 33},
			{60, 45},
		},
		"cnt": {
			{30, 3},
			{60, 3},
		},
	}
	testDecResolutionFakeAvg(t, inData, expectedResult, 10, 30)
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

func TestRowKeyAgg0(t *testing.T) {
	res := getRowKey(0, "aaa", "", 0)
	if res != "aaa" {
		t.Fatalf("row key for aggregation 0 should equal the id")
	}
}

func TestRowKeyAgg1(t *testing.T) {
	res := getRowKey(1, "aaa", "sum", 60)
	if res != "aaa_sum_60" {
		t.Fatalf("row key for aggregation 0 should equal the id")
	}
}

func TestEncodedChunksFromPointsWithoutUnfinished(t *testing.T) {
	points := generatePoints(25200, 10, 10, 0, 8640, func(i float64) float64 { return i + 1 })

	*writeUnfinishedChunks = false
	chunks := encodedChunksFromPoints(points, 10, 21600)

	if len(chunks) != 4 {
		t.Fatalf("Expected to get 4 chunks, but got %d", len(chunks))
	}

	i := 0
	for _, c := range chunks {
		iter, err := c.Get()
		if err != nil {
			t.Fatalf("Error getting iterator: %s", err)
		}
		for iter.Next() {
			ts, val := iter.Values()
			if points[i].Timestamp != ts || points[i].Value != val {
				t.Fatalf("Unexpected value at index %d: %d:%f instead of %d:%f", i, ts, val, points[i].Timestamp, points[i].Value)
			}
			i++
		}
	}
}

func TestEncodedChunksFromPointsWithUnfinished(t *testing.T) {
	points := generatePoints(25200, 10, 10, 0, 8640, func(i float64) float64 { return i + 1 })

	*writeUnfinishedChunks = true
	chunks := encodedChunksFromPoints(points, 10, 21600)

	if len(chunks) != 5 {
		t.Fatalf("Expected to get 5 chunks, but got %d", len(chunks))
	}

	i := 0
	for _, c := range chunks {
		iter, err := c.Get()
		if err != nil {
			t.Fatalf("Error getting iterator: %s", err)
		}
		for iter.Next() {
			ts, val := iter.Values()
			if points[i].Timestamp != ts || points[i].Value != val {
				t.Fatalf("Unexpected value at index %d: %d:%f instead of %d:%f", i, ts, val, points[i].Timestamp, points[i].Value)
			}
			i++
		}
	}
}

func verifyPointMaps(t *testing.T, points map[string][]whisper.Point, expected map[string][]whisper.Point) {
	for meth, ep := range expected {
		if _, ok := points[meth]; !ok {
			t.Fatalf("Missing method %s in result", meth)
		}
		if len(ep) != len(points[meth]) {
			t.Fatalf("Unexpected data returned in %s:\n%+v\n%+v", meth, expected[meth], points[meth])
		}
		for i, p := range ep {
			if points[meth][i] != p {
				t.Fatalf("Unexpected data returned in %s:\n%+v\n%+v", meth, expected[meth], points[meth])
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
		method: "sum",
	}

	expectedPoints1 := map[string][]whisper.Point{
		"sum": {
			{1503384101, 100},
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
	expectedPoints2 := map[string][]whisper.Point{
		"sum": {
			{1503384102, 200},
			{1503384104, 200},
			{1503384106, 200},
			{1503384108, 200},
		},
	}
	expectedPoints3 := map[string][]whisper.Point{
		"sum": {
			{1503384104, 400},
			{1503384108, 200},
		},
	}

	points1 := c.getPoints(0, 1, 8)
	points2 := c.getPoints(0, 2, 8)
	points3 := c.getPoints(0, 4, 8)

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
		method: "lst",
	}

	expectedPoints1 := map[string][]whisper.Point{
		"lst": {
			{1503475273, 5},
			{1503475274, 5},
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
	expectedPoints2 := map[string][]whisper.Point{
		"lst": {
			{1503475274, 5},
			{1503475276, 5},
			{1503475278, 8},
			{1503475280, 7},
			{1503475282, 8},
		},
	}
	expectedPoints3 := map[string][]whisper.Point{
		"lst": {
			{1503475276, 5},
			{1503475280, 8},
		},
	}

	points1 := c.getPoints(0, 1, 8)
	points2 := c.getPoints(0, 2, 8)
	points3 := c.getPoints(0, 4, 8)

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
		method: "sum",
	}

	expectedPoints1 := map[string][]whisper.Point{
		"sum": {
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

	expectedPoints2 := map[string][]whisper.Point{
		"sum": {
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

	expectedPoints3 := map[string][]whisper.Point{
		"sum": {
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
		method: "avg",
	}

	expectedPoints1_0 := map[string][]whisper.Point{
		"avg": {
			{1503407717, 3.5},
			{1503407718, 3.5},
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
	expectedPoints2_0 := map[string][]whisper.Point{
		"avg": {
			{1503407718, 3.5},
			{1503407720, 3.5},
			{1503407722, 7.25},
			{1503407724, 6.5},
			{1503407726, 8},
		},
	}
	expectedPoints3_0 := map[string][]whisper.Point{
		"avg": {
			{1503407720, 3.5},
			{1503407724, 7.25},
		},
	}

	expectedPoints1_1 := map[string][]whisper.Point{
		"sum": {
			{1503407717, 3.5 / 4},
			{1503407718, 3.5 / 4},
			{1503407719, 3.5 / 4},
			{1503407720, 3.5 / 4},
			{1503407721, 7.25 / 4},
			{1503407722, 7.25 / 4},
			{1503407723, 6.5 / 2},
			{1503407724, 6.5 / 2},
			{1503407725, 7},
			{1503407726, 8},
		},
		"cnt": {
			{1503407717, float64(1) / 4},
			{1503407718, float64(1) / 4},
			{1503407719, float64(1) / 4},
			{1503407720, float64(1) / 4},
			{1503407721, float64(1) / 4},
			{1503407722, float64(1) / 4},
			{1503407723, float64(1) / 2},
			{1503407724, float64(1) / 2},
			{1503407725, 1},
			{1503407726, 1},
		},
	}
	expectedPoints2_1 := map[string][]whisper.Point{
		"sum": {
			{1503407718, 3.5 / 2},
			{1503407720, 3.5 / 2},
			{1503407722, 7.25 / 2},
			{1503407724, 6.5},
			{1503407726, 8},
		},
		"cnt": {
			{1503407718, float64(1) / 2},
			{1503407720, float64(1) / 2},
			{1503407722, float64(1) / 2},
			{1503407724, 1},
			{1503407726, 1},
		},
	}
	expectedPoints3_1 := map[string][]whisper.Point{
		"sum": {
			{1503407720, 3.5},
			{1503407724, 7.25},
		},
		"cnt": {
			{1503407720, 1},
			{1503407724, 1},
		},
	}

	points1_0 := c.getPoints(0, 1, 8)
	points2_0 := c.getPoints(0, 2, 8)
	points3_0 := c.getPoints(0, 4, 8)

	points1_1 := c.getPoints(1, 1, 8)
	points2_1 := c.getPoints(1, 2, 8)
	points3_1 := c.getPoints(1, 4, 8)

	verifyPointMaps(t, points1_0, expectedPoints1_0)
	verifyPointMaps(t, points2_0, expectedPoints2_0)
	verifyPointMaps(t, points3_0, expectedPoints3_0)

	verifyPointMaps(t, points1_1, expectedPoints1_1)
	verifyPointMaps(t, points2_1, expectedPoints2_1)
	verifyPointMaps(t, points3_1, expectedPoints3_1)
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
		method: "avg",
	}

	expectedPoints1_0 := map[string][]whisper.Point{
		"avg": {
			{1503406117, 9},
			{1503406118, 9},
			{1503406119, 9},
			{1503406120, 9},
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

	expectedPoints1_1 := map[string][]whisper.Point{
		"sum": {
			{1503406117, float64(9) / 9},
			{1503406118, float64(9) / 9},
			{1503406119, float64(9) / 9},
			{1503406120, float64(9) / 9},
			{1503406121, float64(9) / 9},
			{1503406122, float64(9) / 9},
			{1503406123, float64(9) / 9},
			{1503406124, float64(9) / 9},
			{1503406125, float64(9) / 9},
			{1503406126, float64(18) / 9},
			{1503406127, float64(18) / 9},
			{1503406128, float64(18) / 9},
			{1503406129, float64(18) / 9},
			{1503406130, float64(18) / 9},
			{1503406131, float64(18) / 9},
			{1503406132, float64(18) / 9},
			{1503406133, float64(18) / 9},
			{1503406134, float64(18) / 9},
			{1503406135, float64(25.25) / 9},
			{1503406136, float64(25.25) / 9},
			{1503406137, float64(25.25) / 9},
			{1503406138, float64(21) / 3},
			{1503406139, float64(21) / 3},
			{1503406140, float64(21) / 3},
			{1503406141, float64(24) / 3},
			{1503406142, float64(24) / 3},
			{1503406143, float64(24) / 3},
			{1503406144, float64(26.5) / 3},
			{1503406145, 25},
			{1503406146, 26},
			{1503406147, 27},
		},
		"cnt": {
			{1503406117, float64(1) / 9},
			{1503406118, float64(1) / 9},
			{1503406119, float64(1) / 9},
			{1503406120, float64(1) / 9},
			{1503406121, float64(1) / 9},
			{1503406122, float64(1) / 9},
			{1503406123, float64(1) / 9},
			{1503406124, float64(1) / 9},
			{1503406125, float64(1) / 9},
			{1503406126, float64(1) / 9},
			{1503406127, float64(1) / 9},
			{1503406128, float64(1) / 9},
			{1503406129, float64(1) / 9},
			{1503406130, float64(1) / 9},
			{1503406131, float64(1) / 9},
			{1503406132, float64(1) / 9},
			{1503406133, float64(1) / 9},
			{1503406134, float64(1) / 9},
			{1503406135, float64(1) / 9},
			{1503406136, float64(1) / 9},
			{1503406137, float64(1) / 9},
			{1503406138, float64(1) / 3},
			{1503406139, float64(1) / 3},
			{1503406140, float64(1) / 3},
			{1503406141, float64(1) / 3},
			{1503406142, float64(1) / 3},
			{1503406143, float64(1) / 3},
			{1503406144, float64(1) / 3},
			{1503406145, 1},
			{1503406146, 1},
			{1503406147, 1},
		},
	}

	expectedPoints2_0 := map[string][]whisper.Point{
		"avg": {
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

	expectedPoints2_1 := map[string][]whisper.Point{
		"sum": {
			{1503406119, float64(9) / 3},
			{1503406122, float64(9) / 3},
			{1503406125, float64(9) / 3},
			{1503406128, float64(18) / 3},
			{1503406131, float64(18) / 3},
			{1503406134, float64(18) / 3},
			{1503406137, float64(25.25) / 3},
			{1503406140, 21},
			{1503406143, 24},
			{1503406146, 26.5},
		},
		"cnt": {
			{1503406119, float64(1) / 3},
			{1503406122, float64(1) / 3},
			{1503406125, float64(1) / 3},
			{1503406128, float64(1) / 3},
			{1503406131, float64(1) / 3},
			{1503406134, float64(1) / 3},
			{1503406137, float64(1) / 3},
			{1503406140, 1},
			{1503406143, 1},
			{1503406146, 1},
		},
	}

	expectedPoints3_0 := map[string][]whisper.Point{
		"avg": {
			{1503406125, 9},
			{1503406134, 18},
			{1503406143, 25.25},
		},
	}

	expectedPoints3_1 := map[string][]whisper.Point{
		"sum": {
			{1503406125, 9},
			{1503406134, 18},
			{1503406143, 25.25},
		},
		"cnt": {
			{1503406125, 1},
			{1503406134, 1},
			{1503406143, 1},
		},
	}

	points1_0 := c.getPoints(0, 1, 27)
	points2_0 := c.getPoints(0, 3, 27)
	points3_0 := c.getPoints(0, 9, 27)

	points1_1 := c.getPoints(1, 1, 27)
	points2_1 := c.getPoints(1, 3, 27)
	points3_1 := c.getPoints(1, 9, 27)

	verifyPointMaps(t, points1_0, expectedPoints1_0)
	verifyPointMaps(t, points2_0, expectedPoints2_0)
	verifyPointMaps(t, points3_0, expectedPoints3_0)

	verifyPointMaps(t, points1_1, expectedPoints1_1)
	verifyPointMaps(t, points2_1, expectedPoints2_1)
	verifyPointMaps(t, points3_1, expectedPoints3_1)
}
