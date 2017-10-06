package idx

import (
	"testing"
)

func TestMetricIDStringConversion(t *testing.T) {
	id := MetricID{}
	testIDs := []string{
		"1.abcdefabcdef12340123456789abcdef",
		"12345.00000000000000000000000000000000",
		"12345.aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	}

	for _, idStr := range testIDs {
		err := id.FromString(idStr)
		if err != nil {
			t.Fatalf("Error parsing id %s: %s", idStr, err)
		}
		idStr2 := id.ToString()
		if idStr != idStr2 {
			t.Fatalf("Converted id string has changed: %s %s", idStr, idStr2)
		}
	}
}

func TestMetricIDStringConversionErrors(t *testing.T) {
	id := MetricID{}
	testIDs := []string{
		"abc.00000000000000000000000000000000",
		"1.1111111111111111111111111111111",
		"1.111111111111111111111111111111111",
		"1.g1111111111111111111111111111111",
	}

	for _, idStr := range testIDs {
		err := id.FromString(idStr)
		if err == nil {
			t.Fatalf("Expected error, but didn't get one with %s", idStr)
		}
	}
}
