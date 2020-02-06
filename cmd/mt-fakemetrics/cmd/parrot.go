package cmd

import (
	"fmt"
	"github.com/grafana/metrictank/schema"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"time"
)

var (
	parrotOrgId          int
	parrotPartitionCount int32
	partitionMethod      schema.PartitionByMethod
)

func init() {
	rootCmd.AddCommand(parrotCmd)
	parrotCmd.Flags().IntVar(&parrotOrgId, "parrot-org-id", 1, "org id to publish parrot metrics to")
	parrotCmd.Flags().Int32Var(&parrotPartitionCount, "parrot-partition-count", 128, "number of partitions to publish parrot metrics to")
}

var parrotCmd = &cobra.Command{
	Use:   "parrot",
	Short: "generate deterministic metrics for each metrictank partition",
	Run: func(cmd *cobra.Command, args []string) {
		partitionMethod = parseParrotPartitionMethod()
		if orgs > 1 {
			log.Fatal("parrot only works in single org mode")
		}
		initStats(false, "parrot")
		checkOutputs()
		outputs := getOutputs()
		period = int(periodDur.Seconds())

		metrics := generateParrotMetrics(parrotPartitionCount)

		for tick := range time.NewTicker(time.Duration(period) * time.Second).C {
			for _, metric := range metrics {
				metric.Time = tick.Unix()
				metric.Value = float64(tick.Unix())
			}
			for _, out := range outputs {
				out.Flush(metrics)
			}
		}
	},
}

//parseParrotPartitionMethod parses the partitionScheme cli flag,
//exiting if an invalid partition schema is entered or if org based partitioning is used (not currently allowed by parrot).
func parseParrotPartitionMethod() schema.PartitionByMethod {
	method, err := schema.PartitonMethodFromString(partitionScheme)
	if err != nil {
		log.Fatal(err)
	}
	if partitionMethod == schema.PartitionByOrg {
		log.Fatal("byOrg not supported")
	}
	return method
}

//generateParrotMetrics generates a MetricData that hashes to each of numPartitions partitions
func generateParrotMetrics(numPartitions int32) []*schema.MetricData {
	var metrics []*schema.MetricData
	for i := int32(0); i < numPartitions; i++ {
		metrics = append(metrics, generateParrotMetric(i))
	}
	return metrics
}

//generateParrotMetric generates a single MetricData that hashes to the given partition
func generateParrotMetric(desiredPartition int32) *schema.MetricData {
	metric := schema.MetricData{
		OrgId: parrotOrgId,
		Unit:  "partyparrots",
		Mtype: "gauge",
		Tags:  []string{"partition", fmt.Sprintf("%d", desiredPartition)},
	}

	for i := 1; true; i++ {
		metric.Name = fmt.Sprintf("parrot.testdata.%d.generated.%s", desiredPartition, generatePartitionSuffix(i))
		id, err := metric.PartitionID(partitionMethod, parrotPartitionCount)
		if err != nil {
			log.Fatal(err)
		}
		if id == desiredPartition {
			return &metric
		}
	}
	return nil
}

var alphabet = []rune("abcdefghijklmnopqrstuvwxyz")

//deterministically generate a suffix for partition brute forcing
func generatePartitionSuffix(i int) string {
	if i == 0 {
		return ""
	}
	return generatePartitionSuffix(i/26) + string(alphabet[i%26])
}
