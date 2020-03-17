package main

import (
	"github.com/grafana/metrictank/cmd/mt-fakemetrics/out"
	"github.com/grafana/metrictank/cmd/mt-fakemetrics/out/gnet"
	"github.com/grafana/metrictank/logger"
	"github.com/grafana/metrictank/schema"
	"github.com/grafana/metrictank/stats"
	"github.com/raintank/met/statsd"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"os"
	"strings"
	"time"
)

var (
	gatewayAddress        string
	gatewayKey            string
	orgId                 int
	partitionCount        int32
	partitionMethodString string
	testMetricsInterval   time.Duration
	queryInterval         time.Duration
	lookbackPeriod        time.Duration
	logLevel              string
	lastPublish           int64

	statsGraphite   *stats.Graphite
	statsPrefix     string
	statsAddr       string
	statsBufferSize int
	statsTimeout    time.Duration

	partitionMethod schema.PartitionByMethod
	publisher       out.Out
)

func init() {
	parrotCmd.Flags().StringVar(&gatewayAddress, "gateway-address", "http://localhost:6059", "the url of the metrics gateway")
	parrotCmd.Flags().StringVar(&gatewayKey, "gateway-key", "", "the bearer token to include with gateway requests")
	parrotCmd.Flags().IntVar(&orgId, "org-id", 1, "org id to publish parrot metrics to")
	parrotCmd.Flags().Int32Var(&partitionCount, "partition-count", 8, "number of kafka partitions in use")
	parrotCmd.Flags().StringVar(&partitionMethodString, "partition-method", "bySeries", "the partition method in use on the gateway, must be one of bySeries|bySeriesWithTags|bySeriesWithTagsFnv")
	parrotCmd.Flags().DurationVar(&testMetricsInterval, "test-metrics-interval", 10*time.Second, "interval to send test metrics")
	parrotCmd.Flags().DurationVar(&queryInterval, "query-interval", 10*time.Second, "interval to query to validate metrics")
	parrotCmd.Flags().DurationVar(&lookbackPeriod, "lookback-period", 5*time.Minute, "how far to look back when validating metrics")
	parrotCmd.Flags().StringVar(&statsPrefix, "stats-prefix", "", "stats prefix (will add trailing dot automatically if needed)")
	parrotCmd.Flags().StringVar(&statsAddr, "stats-address", "localhost:2003", "address to send monitoring statistics to")
	parrotCmd.Flags().IntVar(&statsBufferSize, "stats-buffer-size", 20000, "how many messages (holding all measurements from one interval) to buffer up in case graphite endpoint is unavailable.")
	parrotCmd.Flags().DurationVar(&statsTimeout, "stats-timeout", time.Second*10, "timeout after which a write is considered not successful")

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
	Use:   "mt-parrot",
	Short: "generate deterministic metrics for each metrictank partition",
	Run: func(cmd *cobra.Command, args []string) {
		lvl, err := log.ParseLevel(logLevel)
		if err != nil {
			log.Fatalf("failed to parse log-level, %s", err.Error())
		}

		validateDurationsInSeconds()
		if int(lookbackPeriod.Seconds())%int(testMetricsInterval.Seconds()) != 0 {
			log.Fatal("lookback period must be evenly divisible by test metrics interval")
		}

		log.SetLevel(lvl)
		parsePartitionMethod()
		initGateway()
		initStats()

		metrics := generateMetrics(partitionCount)
		go produceTestMetrics(metrics)

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
	publisher, err = gnet.New(gatewayAddress+"/metrics", gatewayKey, backend)
	if err != nil {
		log.Fatal(err)
	}
	log.Info("gateway initialized")
}

func initStats() {
	hostname, _ := os.Hostname()
	prefix := strings.Replace(statsPrefix, "$hostname", strings.Replace(hostname, ".", "_", -1), -1)
	//need to use a negative interval so we can manually set the report timestamps
	statsGraphite = stats.NewGraphite(prefix, statsAddr, -1, statsBufferSize, statsTimeout)
}

func validateDurationsInSeconds() {
	if testMetricsInterval%time.Second != 0 {
		log.Fatal("test-metrics-interval must be in seconds")
	}
	if queryInterval%time.Second != 0 {
		log.Fatal("query-interval must be in seconds")
	}
	if lookbackPeriod%time.Second != 0 {
		log.Fatal("lookback-period must be in seconds")
	}
}
