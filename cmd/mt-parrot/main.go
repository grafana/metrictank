package main

import (
	"github.com/grafana/metrictank/cmd/mt-fakemetrics/out"
	"github.com/grafana/metrictank/cmd/mt-fakemetrics/out/gnet"
	"github.com/grafana/metrictank/logger"
	"github.com/grafana/metrictank/schema"
	"github.com/raintank/met/statsd"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"time"
)

var (
	gatewayAddress            string
	gatewayKey                string
	orgId                     int
	partitionCount            int32
	partitionMethodString     string
	artificialMetricsInterval time.Duration
	queryInterval             time.Duration
	logLevel                  string

	partitionMethod schema.PartitionByMethod
	gateway         out.Out
)

func init() {
	parrotCmd.Flags().StringVar(&gatewayAddress, "gateway-address", "http://localhost:6059", "the url of the metrics gateway")
	parrotCmd.Flags().StringVar(&gatewayKey, "gateway-key", "", "the bearer token to include with gateway requests")
	parrotCmd.Flags().IntVar(&orgId, "org-id", 1, "org id to publish parrot metrics to")
	parrotCmd.Flags().Int32Var(&partitionCount, "partition-count", 8, "number of partitions to publish parrot metrics to")
	parrotCmd.Flags().StringVar(&partitionMethodString, "partition-method", "bySeries", "the partition method to use, must be one of bySeries|bySeriesWithTags|bySeriesWithTagsFnv")
	parrotCmd.Flags().DurationVar(&artificialMetricsInterval, "artificial-metrics-interval", 5*time.Second, "interval to send metrics")
	parrotCmd.Flags().DurationVar(&queryInterval, "query-interval", 10*time.Second, "interval to query to validate metrics")

	parrotCmd.Flags().StringVar(&logLevel, "log-level", "info", "log level. panic|fatal|error|warning|info|debug")

	formatter := &logger.TextFormatter{}
	formatter.TimestampFormat = "2006-01-02 15:04:05.000"
	log.SetFormatter(formatter)
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
		lvl, err := log.ParseLevel(logLevel)
		if err != nil {
			log.Fatalf("failed to parse log-level, %s", err.Error())
		}
		log.SetLevel(lvl)
		parsePartitionMethod()
		initGateway()

		schemas := generateSchemas(partitionCount)
		go produceArtificialMetrics(schemas)

		monitor()
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
	gateway, err = gnet.New(gatewayAddress + "/metrics", gatewayKey, backend)
	if err != nil {
		log.Fatal(err)
	}
	log.Info("gateway initialized")
}
