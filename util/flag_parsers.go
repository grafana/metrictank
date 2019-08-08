package util

import (
	"fmt"
	"math"
	"strconv"
	"strings"
)

func ParseIngestAfterFlag(ingestAfterStr string) (uint32, int64, error) {
	orgID := 0
	timestamp := 0
	if len(ingestAfterStr) > 0 {
		ingestAfterParts := strings.Split(ingestAfterStr, ":")
		if len(ingestAfterParts) != 2 {
			return 0, 0, fmt.Errorf("Could not parse ingest-after. %s", ingestAfterStr)
		}
		var err error
		orgID, err = strconv.Atoi(ingestAfterParts[0])
		if err != nil {
			return 0, 0, fmt.Errorf("Could not parse org id from ingest-after. %s", ingestAfterStr)
		}
		if orgID < 0 || orgID > math.MaxUint32 {
			return 0, 0, fmt.Errorf("Org id out of range. %d", orgID)
		}
		timestamp, err = strconv.Atoi(ingestAfterParts[1])
		if err != nil {
			return 0, 0, fmt.Errorf("Could not parse timestamp from ingest-after. %s", ingestAfterStr)
		}
	}
	return uint32(orgID), int64(timestamp), nil
}

func MustParseIngestAfterFlag(ingestAfterStr string) (uint32, int64) {
	orgID, timestamp, err := ParseIngestAfterFlag(ingestAfterStr)
	if err != nil {
		panic(err.Error())
	}
	return orgID, timestamp
}
