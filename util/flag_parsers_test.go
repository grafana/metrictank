package util

import (
	"testing"
)

func TestMustParseIngestAfterFlag(t *testing.T) {
	tests := []struct {
		name              string
		ingestAfterStr    string
		expectedOrgID     uint32
		expectedTimestamp int64
		shouldPanic       bool
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
			defer func() {
				r := recover()
				if r == nil && test.shouldPanic {
					t.Errorf("MustParseIngestAfterFlag() should have paniced but did not")
				}
				if r != nil && !test.shouldPanic {
					t.Errorf("MustParseIngestAfterFlag() panic-ed but should not have = %v", r)
				}
			}()
			orgID, timestamp := MustParseIngestAfterFlag(test.ingestAfterStr)
			if orgID != test.expectedOrgID {
				t.Errorf("MustParseIngestAfterFlag() expected org id %d, got %d", test.expectedOrgID, orgID)
			}
			if timestamp != test.expectedTimestamp {
				t.Errorf("MustParseIngestAfterFlag() expected timestamp %d, got %d", test.expectedOrgID, timestamp)
			}
		})
	}
}
