package util

import (
	"fmt"
	"math"
	"strconv"
	"strings"
)

func ParseIngestFromFlag(ingestFromStr string) (uint32, int64, error) {
	orgID := 0
	timestamp := 0
	if len(ingestFromStr) > 0 {
		ingestFromParts := strings.Split(ingestFromStr, ":")
		if len(ingestFromParts) != 2 {
			return 0, 0, fmt.Errorf("Could not parse ingest-from. %s", ingestFromStr)
		}
		var err error
		orgID, err = strconv.Atoi(ingestFromParts[0])
		if err != nil {
			return 0, 0, fmt.Errorf("Could not parse org id from ingest-from. %s", ingestFromStr)
		}
		if orgID < 0 || orgID > math.MaxUint32 {
			return 0, 0, fmt.Errorf("Org id out of range. %d", orgID)
		}
		timestamp, err = strconv.Atoi(ingestFromParts[1])
		if err != nil {
			return 0, 0, fmt.Errorf("Could not parse timestamp from ingest-from. %s", ingestFromStr)
		}
	}
	return uint32(orgID), int64(timestamp), nil
}

func MustParseIngestFromFlag(ingestFromStr string) (uint32, int64) {
	orgID, timestamp, err := ParseIngestFromFlag(ingestFromStr)
	if err != nil {
		panic(err.Error())
	}
	return orgID, timestamp
}
