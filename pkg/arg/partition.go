package arg

import (
	"fmt"
	"strconv"
	"strings"
)

// ParsePartitions returns an empty slice for '*', or a slice of integers
// in case a csv list was specified, or an error otherwise.
func ParsePartitions(partitionStr string) ([]int32, error) {
	if partitionStr == "*" {
		return nil, nil
	}
	var partitions []int32
	for _, p := range strings.Split(partitionStr, ",") {
		p = strings.TrimSpace(p)

		// handle trailing "," on the list of partitions.
		if p == "" {
			continue
		}

		id, err := strconv.ParseInt(p, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("invalid partition id %q. partitions must be '*' or a comma separated list of int32 partition id's", p)
		}
		partitions = append(partitions, int32(id))
	}
	return partitions, nil
}
