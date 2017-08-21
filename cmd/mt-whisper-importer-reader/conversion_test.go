package main

import (
	"testing"

	"github.com/kisielk/whisper-go/whisper"
	"github.com/raintank/metrictank/conf"
)

/*func testPlanner(t *testing.T, spp, nop uint32, expected plans) {
	archs := []whisper.ArchiveInfo{
		// 1 hour of 1 sec
		{
			Offset:          0,
			SecondsPerPoint: 1,
			Points:          3600,
		},
		// 2 days of 1 min
		{
			Offset:          0,
			SecondsPerPoint: 60,
			Points:          2880,
		},
		// 1 year of 1 hour
		{
			Offset:          0,
			SecondsPerPoint: 3600,
			Points:          8760,
		},
	}

	plan := conversionPlan(spp, nop, archs)
	if len(plan) != len(expected) {
		t.Fatalf("Length of plan does not match expected:\n%+v\n%+v", plan, expected)
	}

	for i, _ := range expected {
		if expected[i].archive != plan[i].archive || expected[i].timeRange != plan[i].timeRange || expected[i].conversion != plan[i].conversion {
			t.Fatalf("Plan does not match expected:\n%+v\n%+v", plan, expected)
		}
	}
}

func TestPlanSingleInputArch(t *testing.T) {
	expected := plans{
		{archive: 1, timeRange: 24 * 60 * 60, conversion: 0},
	}
	testPlanner(t, 60, 24*60, expected)
}

func TestPlanTwoInputArchs(t *testing.T) {
	expected := plans{
		{archive: 0, timeRange: 60 * 60, conversion: -1},
		{archive: 1, timeRange: 12 * 60 * 60, conversion: 1},
	}
	testPlanner(t, 30, 24*60, expected)
}

func TestPlanExceedAvailableArchs(t *testing.T) {
	expected := plans{
		{archive: 2, timeRange: 365 * 24 * 60 * 60, conversion: 0},
	}
	testPlanner(t, 60*60, 2*365*24, expected)
}*/

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
			{10, 5},
			{15, 5},
			{20, 5.5},
			{25, 5.5},
		},
		"cnt": {
			{10, 0.5},
			{15, 0.5},
			{20, 0.5},
			{25, 0.5},
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
			{12, float64(10) / 3},
			{15, float64(10) / 3},
			{18, float64(10) / 3},
			{21, float64(11) / 3},
			{24, float64(11) / 3},
			{27, float64(11) / 3},
			{30, float64(12) / 4},
			{33, float64(12) / 4},
			{36, float64(12) / 4},
			{39, float64(12) / 4},
			{42, float64(13) / 3},
			{45, float64(13) / 3},
			{48, float64(13) / 3},
			{51, float64(14) / 3},
			{54, float64(14) / 3},
			{57, float64(14) / 3},
		},
		"cnt": {
			{12, float64(1) / 3},
			{15, float64(1) / 3},
			{18, float64(1) / 3},
			{21, float64(1) / 3},
			{24, float64(1) / 3},
			{27, float64(1) / 3},
			{30, float64(1) / 4},
			{33, float64(1) / 4},
			{36, float64(1) / 4},
			{39, float64(1) / 4},
			{42, float64(1) / 3},
			{45, float64(1) / 3},
			{48, float64(1) / 3},
			{51, float64(1) / 3},
			{54, float64(1) / 3},
			{57, float64(1) / 3},
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
			{10, 5},
			{15, 5},
			{40, 6.5},
			{45, 6.5},
			{50, 7},
			{55, 7},
		},
		"cnt": {
			{10, 0.5},
			{15, 0.5},
			{40, 0.5},
			{45, 0.5},
			{50, 0.5},
			{55, 0.5},
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
			{40, 6.5},
			{45, 6.5},
			{10, 5},
			{15, 5},
			{50, 7},
			{55, 7},
		},
		"cnt": {
			{40, 0.5},
			{45, 0.5},
			{10, 0.5},
			{15, 0.5},
			{50, 0.5},
			{55, 0.5},
		},
	}

	testIncResolutionFakeAvg(t, inData, expectedResult, 10, 5)
}

func testIncResolution(t *testing.T, inData, expectedResult []whisper.Point, inRes, outRes uint32) {
	outData := incResolution(inData, inRes, outRes)

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
		{10, 10},
		{15, 10},
		{20, 11},
		{25, 11},
	}
	testIncResolution(t, inData, expectedResult, 10, 5)
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
		{12, 10},
		{15, 10},
		{18, 10},
		{21, 11},
		{24, 11},
		{27, 11},
		{30, 12},
		{33, 12},
		{36, 12},
		{39, 12},
		{42, 13},
		{45, 13},
		{48, 13},
		{51, 14},
		{54, 14},
		{57, 14},
	}

	testIncResolution(t, inData, expectedResult, 10, 3)
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
		{10, 10},
		{15, 10},
		{40, 13},
		{45, 13},
		{50, 14},
		{55, 14},
	}

	testIncResolution(t, inData, expectedResult, 10, 5)
}

func TestIncResolutionOutOfOrder(t *testing.T) {
	inData := []whisper.Point{
		{40, 13},
		{10, 10},
		{50, 14},
	}

	expectedResult := []whisper.Point{
		{10, 10},
		{15, 10},
		{40, 13},
		{45, 13},
		{50, 14},
		{55, 14},
	}

	testIncResolution(t, inData, expectedResult, 10, 5)
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

func testAdjustAggregation(t *testing.T, ret conf.Retention, retIdx int, arch whisper.ArchiveInfo, meth string, points []whisper.Point, expectedRes map[string][]whisper.Point) {
	res := adjustAggregation(ret, retIdx, arch, meth, points)
	for meth, expectedPoints := range expectedRes {
		if _, ok := res[meth]; !ok {
			t.Fatalf("missing expected agg method %s in %+v", meth, res)
		}

		if len(res[meth]) != len(expectedPoints) {
			t.Fatalf("length of %s does not match expected:\n%+v\n%+v", meth, res, expectedRes)
		}

		for i, _ := range expectedPoints {
			if expectedPoints[i] != res[meth][i] {
				t.Fatalf("point %d of %s does not match expected:\n%+v\n%+v", i, meth, res, expectedRes)
			}
		}
	}
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

func TestAdjustAggregationDecResolution(t *testing.T) {
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
