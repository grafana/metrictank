package kafka

import (
	confluent "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/twinj/uuid"
)

// according to this we need to generate a uuid to set as group.id:
// https://github.com/edenhill/librdkafka/issues/1210

func GetConfig(broker, compression string, batchNumMessages, bufferMaxMs, bufferSize, fetchMin, maxOpenRequests, maxWait, sessionTimeout int) *confluent.ConfigMap {
	return &confluent.ConfigMap{
		"bootstrap.servers":                     broker,
		"compression.codec":                     "snappy",
		"group.id":                              uuid.NewV4().String(),
		"fetch.min.bytes":                       fetchMin,
		"fetch.wait.max.ms":                     maxWait,
		"max.in.flight.requests.per.connection": maxOpenRequests,
		"queue.buffering.max.messages":          bufferSize,
		"retries":                               10,
		"session.timeout.ms":                    sessionTimeout,
		"queue.buffering.max.ms":                bufferMaxMs,
		"batch.num.messages":                    batchNumMessages,
	}
}
