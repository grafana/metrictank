package main

import (
	"fmt"
	"sync/atomic"

	"github.com/grafana/metrictank/internal/clock"
	"github.com/grafana/metrictank/internal/schema"
	log "github.com/sirupsen/logrus"
)

func produceTestMetrics(metrics []*schema.MetricData) {
	for tick := range clock.AlignedTickLossless(testMetricsInterval) {
		for _, metric := range metrics {
			metric.Time = tick.Unix()
			metric.Value = float64(tick.Unix())
		}
		publisher.Flush(metrics)
		atomic.StoreInt64(&lastPublish, tick.Unix())
		log.Infof("flushed metrics for ts %d", tick.Unix())
	}
}

// generateMetrics generates a MetricData that hashes to each of numPartitions partitions
func generateMetrics(numPartitions int32) []*schema.MetricData {
	var metrics []*schema.MetricData
	for i := int32(0); i < numPartitions; i++ {
		metrics = append(metrics, generateMetric(i))
	}
	return metrics
}

// generateMetric generates a single MetricData that hashes to the given partition
func generateMetric(desiredPartition int32) *schema.MetricData {
	metric := schema.MetricData{
		OrgId:    orgId,
		Unit:     "partyparrots",
		Mtype:    "gauge",
		Interval: int(testMetricsInterval.Seconds()),
	}

	for i := 1; true; i++ {
		metric.Name = fmt.Sprintf("parrot.testdata.%d.identity.%s", desiredPartition, generatePartitionSuffix(i))
		id, err := metric.PartitionID(partitionMethod, partitionCount)
		if err != nil {
			log.Fatal(err)
		}
		if id == desiredPartition {
			log.Infof("metric for partition %d: %s", desiredPartition, metric.Name)
			return &metric
		}
	}
	return nil
}

var alphabet = []rune("abcdefghijklmnopqrstuvwxyz")

// generatePartitionSuffix deterministically generates a suffix for partition by brute force
func generatePartitionSuffix(i int) string {
	if i > 25 {
		return generatePartitionSuffix((i/26)-1) + string(alphabet[i%26])
	}
	return string(alphabet[i%26])
}
