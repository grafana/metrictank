package main

import (
	"flag"
	"fmt"
	l "log"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/Shopify/sarama"

	"github.com/grafana/metrictank/cluster"
	inKafkaMdm "github.com/grafana/metrictank/input/kafkamdm"
	statsConfig "github.com/grafana/metrictank/stats/config"
	"github.com/raintank/worldping-api/pkg/log"
	"github.com/rakyll/globalconf"
)

var (
	GitHash           = "(none)"
	showVersion       = flag.Bool("version", false, "print version string")
	logLevel          = flag.Int("log-level", 2, "log level. 0=TRACE|1=DEBUG|2=INFO|3=WARN|4=ERROR|5=CRITICAL|6=FATAL")
	confFile          = flag.String("config", "", "configuration file path")
	instance          = flag.String("instance", "default", "instance identifier. must be unique. Used for naming queue consumers and emitted metrics")
	destinationURL    = flag.String("destination-url", "http://localhost/metrics", "tsdb-gw address to send metrics to")
	destinationKey    = flag.String("destination-key", "admin-key", "admin-key of destination tsdb-gw server")
	producerBatchSize = flag.Int("batch-size", 10000, "number of metrics to send in each batch.")
)

func main() {
	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "mt-replicator-via-tsdb")
		fmt.Fprintln(os.Stderr)
		fmt.Fprintln(os.Stderr, "Replicates a kafka mdm topic on a given cluster to a remote tsdb-gw server")
		fmt.Fprintf(os.Stderr, "\nFlags:\n\n")
		flag.PrintDefaults()
	}
	flag.Parse()

	// Only try and parse the conf file if it exists
	path := ""
	if *confFile != "" {
		if _, err := os.Stat(*confFile); err == nil {
			path = *confFile
		}
	}
	config, err := globalconf.NewWithOptions(&globalconf.Options{
		Filename:  path,
		EnvPrefix: "MT_",
	})
	if err != nil {
		log.Fatal(4, "error with configuration file: %s", err)
		os.Exit(1)
	}

	if *showVersion {
		fmt.Printf("mt-replicator-via-tsdb (built with %s, git hash %s)\n", runtime.Version(), GitHash)
		return
	}

	inKafkaMdm.ConfigSetup()
	statsConfig.ConfigSetup()
	config.ParseAll()

	log.NewLogger(0, "console", fmt.Sprintf(`{"level": %d, "formatting":false}`, *logLevel))
	sarama.Logger = l.New(os.Stdout, "[Sarama] ", l.LstdFlags)
	inKafkaMdm.LogLevel = *logLevel
	switch *logLevel {
	case 0:
		log.Level(log.TRACE)
	case 1:
		log.Level(log.DEBUG)
	case 2:
		log.Level(log.INFO)
	case 3:
		log.Level(log.WARN)
	case 4:
		log.Level(log.ERROR)
	case 5:
		log.Level(log.CRITICAL)
	case 6:
		log.Level(log.FATAL)
	}

	if *instance == "" {
		log.Fatal(4, "instance can't be empty")
	}

	// initialize the clusterManager so that we can query it for our partitions.
	cluster.Mode = cluster.ModeSingle
	cluster.Init(*instance, GitHash, time.Now(), "", 0)

	inKafkaMdm.ConfigProcess(*instance)
	statsConfig.ConfigProcess(*instance)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	statsConfig.Start()

	consumer := inKafkaMdm.New()
	replicator := NewMetricsReplicator(*destinationURL, *destinationKey)
	replicator.Start()
	log.Info("starting metrics replicator")
	consumer.Start(replicator)

	<-sigChan
	log.Info("metrics replicator shutdown started.")
	consumer.Stop()
	replicator.Stop()
	log.Info("shutdown complete")
	log.Close()
}
