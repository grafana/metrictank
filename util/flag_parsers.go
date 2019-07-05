package util

import (
	"fmt"
	"math"
	"strconv"
	"strings"
)

func MustParseIngestAfterFlag(ingestAfterStr string) (uint32, int64) {
	orgID := 0
	timestamp := 0
	if len(ingestAfterStr) > 0 {
		ingestAfterParts := strings.Split(ingestAfterStr, ":")
		if len(ingestAfterParts) != 2 {
			panic(fmt.Sprintf("Could not parse ingest-after. %s", ingestAfterStr))
		}
		var err error
		orgID, err = strconv.Atoi(ingestAfterParts[0])
		if err != nil {
			panic(fmt.Sprintf("Could not parse org id from ingest-after. %s", ingestAfterStr))
		}
		if orgID < 0 || orgID > math.MaxUint32 {
			panic(fmt.Sprintf("Org id out of range. %d", orgID))
		}
		timestamp, err = strconv.Atoi(ingestAfterParts[1])
		if err != nil {
			panic(fmt.Sprintf("Could not parse timestamp from ingest-after. %s", ingestAfterStr))
		}
	}
	return uint32(orgID), int64(timestamp)
}
