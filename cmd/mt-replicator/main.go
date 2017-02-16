package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"sync"
	"syscall"

	"github.com/raintank/worldping-api/pkg/log"
)

var (
	GitHash     = "(none)"
	showVersion = flag.Bool("version", false, "print version string")
	logLevel    = flag.Int("log-level", 2, "log level. 0=TRACE|1=DEBUG|2=INFO|3=WARN|4=ERROR|5=CRITICAL|6=FATAL")

	partitionScheme  = flag.String("partition-scheme", "bySeries", "method used for partitioning metrics. (byOrg|bySeries)")
	compression      = flag.String("compression", "snappy", "compression: none|gzip|snappy")
	replicateMetrics = flag.Bool("metrics", false, "replicate metrics")
	replicatePersist = flag.Bool("persist", false, "replicate persistMetrics")
	group            = flag.String("group", "mt-replicator", "Kafka consumer group")
	metricSrcTopic   = flag.String("metric-src-topic", "mdm", "metrics topic name on source cluster")
	metricDstTopic   = flag.String("metric-dst-topic", "mdm", "metrics topic name on destination cluster")
	persistSrcTopic  = flag.String("persist-src-topic", "metricpersist", "metricPersist topic name on source cluster")
	persistDstTopic  = flag.String("persist-dst-topic", "metricpersist", "metricPersist topic name on destination cluster")
	initialOffset    = flag.Int("initial-offset", -2, "initial offset to consume from. (-2=oldest, -1=newest)")
	srcBrokerStr     = flag.String("src-brokers", "localhost:9092", "tcp address of source kafka cluster (may be be given multiple times as a comma-separated list)")
	dstBrokerStr     = flag.String("dst-brokers", "localhost:9092", "tcp address for kafka cluster to consume from (may be be given multiple times as a comma-separated list)")
)

func main() {
	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "mt-replicator")
		fmt.Fprintln(os.Stderr)
		fmt.Fprintln(os.Stderr, "Replicates a kafka mdm topic on a given cluster to a topic on another")
		fmt.Fprintf(os.Stderr, "\nFlags:\n\n")
		flag.PrintDefaults()
	}
	flag.Parse()
	log.NewLogger(0, "console", fmt.Sprintf(`{"level": %d, "formatting":false}`, *logLevel))

	if *showVersion {
		fmt.Printf("mt-replicator (built with %s, git hash %s)\n", runtime.Version(), GitHash)
		return
	}

	if !*replicateMetrics && !*replicatePersist {
		log.Fatal(4, "at least one of --metrics or --persist is needed.")
	}

	if *group == "" {
		log.Fatal(4, "--group is required")
	}

	if *srcBrokerStr == "" {
		log.Fatal(4, "--src-brokers required")
	}
	if *dstBrokerStr == "" {
		log.Fatal(4, "--dst-brokers required")
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	srcBrokers := strings.Split(*srcBrokerStr, ",")
	dstBrokers := strings.Split(*dstBrokerStr, ",")
	wg := new(sync.WaitGroup)
	shutdown := make(chan struct{})
	if *replicateMetrics {
		if *metricSrcTopic == "" {
			log.Fatal(4, "--metric-src-topic is required")
		}

		if *metricDstTopic == "" {
			log.Fatal(4, "--metric-dst-topic is required")
		}

		metrics, err := NewMetricsReplicator(srcBrokers, dstBrokers, *compression, *group, *metricSrcTopic, *metricDstTopic, *initialOffset, *partitionScheme)
		if err != nil {
			log.Fatal(4, err.Error())
		}

		log.Info("starting metrics replicator")
		metrics.Start()
		wg.Add(1)
		go func() {
			<-shutdown
			log.Info("metrics replicator shutdown started.")
			metrics.Stop()
			log.Info("metrics replicator ended.")
			wg.Done()
		}()
	}

	if *replicatePersist {
		if *persistSrcTopic == "" {
			log.Fatal(4, "--persist-src-topic is required")
		}

		if *persistDstTopic == "" {
			log.Fatal(4, "--persist-dst-topic is required")
		}

		metricPersist, err := NewPersistReplicator(srcBrokers, dstBrokers, *group, *persistSrcTopic, *persistDstTopic, *initialOffset)
		if err != nil {
			log.Fatal(4, err.Error())
		}

		log.Info("starting metricPersist replicator")
		metricPersist.Start()
		wg.Add(1)
		go func() {
			<-shutdown
			log.Info("metricPersist replicator shutdown started.")
			metricPersist.Stop()
			log.Info("metricPersist replicator ended.")
			wg.Done()
		}()
	}

	<-sigChan
	close(shutdown)
	wg.Wait()
	log.Info("shutdown complete")
	log.Close()
}
