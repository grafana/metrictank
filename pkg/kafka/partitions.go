package kafka

import (
	"fmt"

	"github.com/Shopify/sarama"
)

// returns elements that are in a but not in b
func DiffPartitions(a []int32, b []int32) []int32 {
	var diff []int32
Iter:
	for _, eA := range a {
		for _, eB := range b {
			if eA == eB {
				continue Iter
			}
		}
		diff = append(diff, eA)
	}
	return diff
}

func GetPartitions(client sarama.Client, topics []string) ([]int32, error) {
	partitionCount := 0
	partitions := make([]int32, 0)
	var err error
	for i, topic := range topics {
		partitions, err = client.Partitions(topic)
		if err != nil {
			return nil, fmt.Errorf("Failed to get partitions for topic %s. %s", topic, err)
		}
		if len(partitions) == 0 {
			return nil, fmt.Errorf("No partitions returned for topic %s", topic)
		}
		if i > 0 {
			if len(partitions) != partitionCount {
				return nil, fmt.Errorf("Configured topics have different partition counts, this is not supported")
			}
			continue
		}
		partitionCount = len(partitions)
	}
	return partitions, nil
}
