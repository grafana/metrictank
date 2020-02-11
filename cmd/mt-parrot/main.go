package main

import (
	"fmt"
	"github.com/grafana/metrictank/cmd/mt-fakemetrics/out"
	"github.com/grafana/metrictank/cmd/mt-fakemetrics/out/gnet"
	"github.com/grafana/metrictank/schema"
	"github.com/raintank/met/statsd"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"time"
)

var (
	gatewayAddress        string
	gatewayKey            string
	orgId                 int
	partitionCount        int32
	partitionMethodString string
	period                time.Duration

	partitionMethod schema.PartitionByMethod
	gateway         out.Out
)

func init() {
	parrotCmd.Flags().StringVar(&gatewayAddress, "gateway-address", "http://localhost:5059", "the url of the metrics gateway to publish to")
	parrotCmd.Flags().StringVar(&gatewayKey, "gateway-key", "", "the bearer token to include with gateway requests")
	parrotCmd.Flags().IntVar(&orgId, "org-id", 1, "org id to publish parrot metrics to")
	parrotCmd.Flags().Int32Var(&partitionCount, "partition-count", 128, "number of partitions to publish parrot metrics to")
	parrotCmd.Flags().StringVar(&partitionMethodString, "partition-method", "bySeries", "the partition method to use, must be one of bySeries|bySeriesWithTags|bySeriesWithTagsFnv")
	parrotCmd.Flags().DurationVar(&period, "period", time.Second, "interval to send metrics")
}

func main() {
	err := parrotCmd.Execute()
	if err != nil {
		log.Fatal(err)
	}
}

var parrotCmd = &cobra.Command{
	Use:   "parrot",
	Short: "generate deterministic metrics for each metrictank partition",
	Run: func(cmd *cobra.Command, args []string) {
		parsePartitionMethod()
		initGateway()
		fmt.Println("gateway initialized")

		metrics := generateMetrics(partitionCount)

		for tick := range time.NewTicker(period).C {
			for _, metric := range metrics {
				metric.Time = tick.Unix()
				metric.Value = float64(tick.Unix())
			}
			gateway.Flush(metrics)
		}
	},
}

//parsePartitionMethod parses the partitionScheme cli flag,
//exiting if an invalid partition schema is entered or if org based partitioning is used (not currently allowed by parrot).
func parsePartitionMethod() {
	var err error
	partitionMethod, err = schema.PartitonMethodFromString(partitionMethodString)
	if err != nil {
		log.Fatal(err)
	}
	if partitionMethod == schema.PartitionByOrg {
		log.Fatal("byOrg not supported")
	}
}

func initGateway() {
	var err error
	backend, _ := statsd.New(false, "", "")
	gateway, err = gnet.New(gatewayAddress, gatewayKey, backend)
	if err != nil {
		log.Fatal(err)
	}
}

//generateParrotMetrics generates a MetricData that hashes to each of numPartitions partitions
func generateMetrics(numPartitions int32) []*schema.MetricData {
	var metrics []*schema.MetricData
	for i := int32(0); i < numPartitions; i++ {
		metrics = append(metrics, generateMetric(i))
	}
	return metrics
}

//generateParrotMetric generates a single MetricData that hashes to the given partition
func generateMetric(desiredPartition int32) *schema.MetricData {
	metric := schema.MetricData{
		OrgId:    orgId,
		Unit:     "partyparrots",
		Mtype:    "gauge",
		Interval: int(period.Seconds()),
		Tags:     []string{fmt.Sprintf("partition=%d", desiredPartition)},
	}

	for i := 1; true; i++ {
		metric.Name = fmt.Sprintf("parrot.testdata.%d.generated.%s", desiredPartition, generatePartitionSuffix(i))
		id, err := metric.PartitionID(partitionMethod, partitionCount)
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
