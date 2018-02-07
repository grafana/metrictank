package kafka

import (
	"fmt"
	"time"

	confluent "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/raintank/worldping-api/pkg/log"
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

func GetPartitions(client *confluent.Consumer, topics []string, retries, backoff, timeout int) ([]int32, error) {
	var partitions []int32
	for i, topic := range topics {
		for retry := retries; retry > 0; retry-- {
			metadata, err := client.GetMetadata(&topic, false, timeout)
			if err != nil {
				log.Warn("kafka: failed to get metadata from kafka client. %s, %d retries", err, retry)
				time.Sleep(time.Duration(backoff) * time.Millisecond)
				continue
			}

			// if kafka's auto.create.topics is enabled (default) then a topic will get created with the default
			// settings after our first GetMetadata call for it. But because the topic creation can take a moment
			// we'll need to retry a fraction of a second later in order to actually get the according metadata.
			tm, ok := metadata.Topics[topic]
			if !ok || tm.Error.Code() == confluent.ErrUnknownTopic {
				log.Warn("kafka: unknown topic %s, %d retries", topic, retry)
				time.Sleep(time.Duration(backoff) * time.Millisecond)
				continue
			}
			if len(tm.Partitions) == 0 {
				log.Warn("kafka: 0 partitions returned for %s, %d retries left, %d backoffMs", topic, retry, backoff)
				time.Sleep(time.Duration(backoff) * time.Millisecond)
				continue
			}

			if i == 0 {
				partitions = make([]int32, 0, len(tm.Partitions))
				for _, partitionMetadata := range tm.Partitions {
					partitions = append(partitions, partitionMetadata.ID)
				}
			} else {
				if len(tm.Partitions) != len(partitions) {
					return nil, fmt.Errorf("Configured topics have different partition counts, this is not supported")
				}
			}
		}
	}

	log.Info("kafka: partitions for topics %+v: %+v", topics, partitions)
	return partitions, nil
}
