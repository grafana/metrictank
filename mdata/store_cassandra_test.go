package mdata

import (
	"fmt"
	"math"
	"testing"
)

type testCase struct {
	ttl                uint32
	windowFactor       int
	nameFormat         string
	expectedTableName  string
	expectedWindowSize uint32
}

const oneSecond = 1
const oneMinute = 60
const oneHour = 60 * 60
const oneDay = 24 * 60 * 60
const oneMonth = oneDay * 30
const oneYear = oneMonth * 12

func TestGetTTLTables(t *testing.T) {
	tcs := []testCase{
		// that's no real world scenario, but let's test it anyway
		{0, 20, "metric_%d", "metric_0", 1},

		{oneSecond, 20, "metric_%d", "metric_0", 1},
		{oneMinute, 20, "metric_%d", "metric_0", 1},
		{oneHour, 20, "metric_%d", "metric_1", 1},
		{oneDay, 20, "metric_%d", "metric_16", 1},
		{oneMonth, 20, "metric_%d", "metric_512", 26},
		{oneYear, 20, "metric_%d", "metric_8192", 410},

		// cases around the edge
		{1024*3600 - 1, 20, "metric_%d", "metric_512", 26},
		{1024 * 3600, 20, "metric_%d", "metric_1024", 52},

		// alternative window factor
		{oneDay, 50, "metric_%d", "metric_16", 1},
		{oneMonth, 50, "metric_%d", "metric_512", 11},
		{oneYear, 50, "metric_%d", "metric_8192", 164},

		// in python3: math.floor(math.pow(2, math.floor(math.log2(math.pow(2,32)/3600)))/100)+1 = 10486
		{math.MaxUint32, 100, "metric_%d", "metric_1048576", 10486},
	}

	for _, tc := range tcs {
		result := getTTLTables([]uint32{tc.ttl}, tc.windowFactor, tc.nameFormat)
		logPrefix := fmt.Sprintf("TTL: %d, WF: %d: ", tc.ttl, tc.windowFactor)
		if len(result) != 1 {
			t.Fatalf("%s expected 1 result, got %d", logPrefix, len(result))
		}
		if result[tc.ttl].table != tc.expectedTableName {
			t.Fatalf("%s expected table name '%s', got '%s'", logPrefix, tc.expectedTableName, result[tc.ttl].table)
		}
		if result[tc.ttl].windowSize != tc.expectedWindowSize {
			t.Fatalf("%s expected window size %d, got %d", logPrefix, tc.expectedWindowSize, result[tc.ttl].windowSize)
		}
	}
}
