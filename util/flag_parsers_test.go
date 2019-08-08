package util

import (
	"testing"
)

func TestParseIngestFromFlag(t *testing.T) {
	tests := []struct {
		name              string
		ingestFromStr     string
		expectedOrgID     uint32
		expectedTimestamp int64
		shouldErr         bool
	}{
		{"empty", "", 0, 0, false},
		{"only zeroes", "0:0", 0, 0, false},
		{"typical", "10:4738913", 10, 4738913, false},
		{"missing timestamp", "0", 0, 0, true},
		{"text no number 1", "text", 0, 0, true},
		{"text no number 2", "1:text", 0, 0, true},
		{"orgid overflow", "10000000000:0", 0, 0, true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			orgID, timestamp, err := ParseIngestFromFlag(test.ingestFromStr)
			if err == nil && test.shouldErr {
				t.Errorf("ParseIngestFromFlag() should have errored but did not")
			}
			if err != nil && !test.shouldErr {
				t.Errorf("ParseIngestFromFlag() errored but should not have = %v", err)
			}
			if orgID != test.expectedOrgID {
				t.Errorf("ParseIngestFromFlag() expected org id %d, got %d", test.expectedOrgID, orgID)
			}
			if timestamp != test.expectedTimestamp {
				t.Errorf("ParseIngestFromFlag() expected timestamp %d, got %d", test.expectedOrgID, timestamp)
			}
		})
	}
}
