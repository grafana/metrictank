package util

import (
	"testing"
)

func TestParseIngestAfterFlag(t *testing.T) {
	tests := []struct {
		name              string
		ingestAfterStr    string
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
			orgID, timestamp, err := ParseIngestAfterFlag(test.ingestAfterStr)
			if err == nil && test.shouldErr {
				t.Errorf("ParseIngestAfterFlag() should have errored but did not")
			}
			if err != nil && !test.shouldErr {
				t.Errorf("ParseIngestAfterFlag() errored but should not have = %v", err)
			}
			if orgID != test.expectedOrgID {
				t.Errorf("ParseIngestAfterFlag() expected org id %d, got %d", test.expectedOrgID, orgID)
			}
			if timestamp != test.expectedTimestamp {
				t.Errorf("ParseIngestAfterFlag() expected timestamp %d, got %d", test.expectedOrgID, timestamp)
			}
		})
	}
}
