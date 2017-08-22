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

/*func TestAdjustAggregationDecResolution(t *testing.T) {
	ret := conf.Retention{
		SecondsPerPoint: 10,
		NumberOfPoints:  15,
	}

	arch := whisper.ArchiveInfo{
		Offset:          5,
		SecondsPerPoint: 30,
		Points:          5,
	}

	points := generatePoints(3600, 30, 10, 3, 5, func(i float64) float64 { return i + 100 })

	expected := map[string][]whisper.Point{
		"lst": {
			{Timestamp: 3600, Value: 10},
			{Timestamp: 3610, Value: 10},
			{Timestamp: 3620, Value: 10},
			{Timestamp: 3630, Value: 110},
			{Timestamp: 3640, Value: 110},
			{Timestamp: 3650, Value: 110},
			{Timestamp: 3660, Value: 210},
			{Timestamp: 3670, Value: 210},
			{Timestamp: 3680, Value: 210},
			{Timestamp: 3690, Value: 310},
			{Timestamp: 3700, Value: 310},
			{Timestamp: 3710, Value: 310},
			{Timestamp: 3720, Value: 410},
			{Timestamp: 3730, Value: 410},
			{Timestamp: 3740, Value: 410},
		},
	}

	testAdjustAggregation(t, ret, 0, arch, "lst", points, expected)
	testAdjustAggregation(t, ret, 3, arch, "lst", points, expected)
}

func TestAdjustAggregationAvg0(t *testing.T) {
	ret := conf.Retention{
		SecondsPerPoint: 30,
		NumberOfPoints:  30,
	}

	arch := whisper.ArchiveInfo{
		Offset:          3,
		SecondsPerPoint: 10,
		Points:          14,
	}

	points := generatePoints(3600, 10, 10, 3, 14, func(i float64) float64 { return i + 1 })

	expected := map[string][]whisper.Point{
		"avg": {
			{Timestamp: 3600, Value: 10},
			{Timestamp: 3630, Value: 12},
			{Timestamp: 3660, Value: 15},
			{Timestamp: 3690, Value: 18},
			{Timestamp: 3720, Value: 21},
		},
	}

	testAdjustAggregation(t, ret, 0, arch, "avg", points, expected)
}

func TestAdjustAggregationAvg1(t *testing.T) {
	ret := conf.Retention{
		SecondsPerPoint: 30,
		NumberOfPoints:  30,
	}

	arch := whisper.ArchiveInfo{
		Offset:          3,
		SecondsPerPoint: 10,
		Points:          12,
	}

	points := generatePoints(3600, 10, 10, 3, 14, func(i float64) float64 { return i + 1 })

	expected := map[string][]whisper.Point{
		"sum": {
			{Timestamp: 3600, Value: 10},
			{Timestamp: 3630, Value: 36},
			{Timestamp: 3660, Value: 45},
			{Timestamp: 3690, Value: 54},
			{Timestamp: 3720, Value: 63},
		},
		"cnt": {
			{Timestamp: 3600, Value: 1},
			{Timestamp: 3630, Value: 3},
			{Timestamp: 3660, Value: 3},
			{Timestamp: 3690, Value: 3},
			{Timestamp: 3720, Value: 3},
		},
	}

	testAdjustAggregation(t, ret, 1, arch, "avg", points, expected)
}*/

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

func TestPointsConversionSimple1(t *testing.T) {
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

	expectedPoints := map[string][]whisper.Point{
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

	points, _ := c.getPoints(0, "sum", 1, 8)

	for meth, ep := range expectedPoints {
		if len(ep) != len(points[meth]) {
			t.Fatalf("Unexpected data returned:\n%+v\n%+v", expectedPoints, points)
		}
		for i, p := range ep {
			if points[meth][i] != p {
				t.Fatalf("Unexpected data returned:\n%+v\n%+v", expectedPoints, points)
			}
		}
	}
}

func TestPointsConversionSimple2(t *testing.T) {
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

	expectedPoints := map[string][]whisper.Point{
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

	points, _ := c.getPoints(0, "sum", 1, 32)

	for meth, ep := range expectedPoints {
		if len(ep) != len(points[meth]) {
			t.Fatalf("Unexpected data returned:\n%+v\n%+v", expectedPoints, points)
		}
		for i, p := range ep {
			if points[meth][i] != p {
				t.Fatalf("Unexpected data returned:\n%+v\n%+v", expectedPoints, points)
			}
		}
	}
}
