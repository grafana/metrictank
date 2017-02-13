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

	partitionScheme  = flag.String("partition-scheme", "byOrg", "method used for partitioning metrics. (byOrg|bySeries)")
	compression      = flag.String("compression", "none", "compression: none|gzip|snappy")
	replicateMetrics = flag.Bool("metrics", false, "replicate metrics")
	replicatePersist = flag.Bool("persist", false, "replicate persistMetrics")
	group            = flag.String("group", "mt-replicator", "Kafka consumer group")
	srcTopic         = flag.String("src-topic", "mdm", "topic name on source cluster")
	dstTopic         = flag.String("dst-topic", "mdm", "topic name on destination cluster")
	persistSrcTopic  = flag.String("persist-src-topic", "metricpersist", "metricPersist topic name on source cluster")
	persistDstTopic  = flag.String("persist-dst-topic", "metricpersist", "metricPersist topic name on destination cluster")
	initialOffset    = flag.Int("initial-offset", -2, "initial offset to consume from. (-2=oldest, -1=newest)")
	srcBrokerStr     = flag.String("src-brokers", "localhost:9092", "tcp address of source kafka cluster (may be be given multiple times as a comma-separated list)")
	dstBrokerStr     = flag.String("dst-brokers", "localhost:9092", "tcp address for kafka cluster to consume from (may be be given multiple times as a comma-separated list)")

	wg sync.WaitGroup
)

type topic struct {
	src string
	dst string
}

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
		fmt.Printf("eventtank (built with %s, git hash %s)\n", runtime.Version(), GitHash)
		return
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	if *group == "" {
		log.Fatal(4, "--group is required")
	}

	if *srcBrokerStr == "" {
		log.Fatal(4, "--src-brokers required")
	}
	if *dstBrokerStr == "" {
		log.Fatal(4, "--dst-brokers required")
	}

	srcBrokers := strings.Split(*srcBrokerStr, ",")
	dstBrokers := strings.Split(*dstBrokerStr, ",")
	wg := new(sync.WaitGroup)

	if *replicateMetrics {

		if *srcTopic == "" {
			log.Fatal(4, "--src-topic is required")
		}

		if *dstTopic == "" {
			log.Fatal(4, "--dst-topic is required")
		}

		consumer, err := NewConsumer(srcBrokers, *group, *srcTopic, *initialOffset)
		if err != nil {
			log.Fatal(4, err.Error())
		}
		publisher, err := NewPublisher(dstBrokers, *dstTopic, *compression, *partitionScheme)
		if err != nil {
			log.Fatal(4, err.Error())
		}
		log.Info("starting metrics consumer")
		consumer.Start(publisher)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-consumer.Done:
					log.Info("metrics consumer ended.")
					publisher.Stop()
					return
				case <-sigChan:
					log.Info("metrics shutdown started.")
					consumer.Stop()
				}
			}
		}()
	}

	if *replicatePersist {
		if *persistSrcTopic == "" {
			log.Fatal(4, "--persist-src-topic is required")
		}

		if *persistDstTopic == "" {
			log.Fatal(4, "--persist-dst-topic is required")
		}

		metricPersist, err := NewPersistRelay(srcBrokers, dstBrokers, *group, *persistSrcTopic, *persistDstTopic, *initialOffset)
		if err != nil {
			log.Fatal(4, err.Error())
		}

		log.Info("starting metricPersist relay")
		metricPersist.Start()
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-metricPersist.Done:
					log.Info("metricPersist ended.")
					return
				case <-sigChan:
					log.Info("metricPersist shutdown started.")
					metricPersist.Stop()
				}
			}
		}()
	}

	wg.Wait()
	log.Info("shutdown complete")
	log.Close()
}
