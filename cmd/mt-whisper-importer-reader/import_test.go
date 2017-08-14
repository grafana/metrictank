package main

import (
	"testing"

	"github.com/kisielk/whisper-go/whisper"
)

func testIncResolution(t *testing.T, inData, expectedResult []whisper.Point, inRes, outRes uint32) {
	outData := incResolution(inData, inRes, outRes)

	if len(expectedResult) != len(outData) {
		t.Fatalf("Generated data has different length (%d) than expected (%d):\n%+v\n%+v", len(outData), len(expectedResult), outData, expectedResult)
	}

	for i := 0; i < len(expectedResult); i++ {
		if outData[i] != expectedResult[i] {
			t.Fatalf("Datapoint does not match expected data:\n%+v\n%+v", outData[i], expectedResult[i])
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
