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

	partitionBy  = flag.String("partition-by", "byOrg", "method used for paritioning metrics. (byOrg|bySeries)")
	compression  = flag.String("compression", "none", "compression: none|gzip|snappy")
	group        = flag.String("group", "mt-replicator", "Kafka consumer group")
	srcTopic     = flag.String("src-topic", "mdm", "topic name on source cluster")
	dstTopic     = flag.String("dst-topic", "mdm", "topic name on destination cluster")
	srcBrokerStr = flag.String("src-brokers", "localhost:9092", "tcp address of source kafka cluster (may be be given multiple times as a comma-separated list)")
	dstBrokerStr = flag.String("dst-brokers", "localhost:9092", "tcp address for kafka cluster to consume from (may be be given multiple times as a comma-separated list)")

	wg sync.WaitGroup
)

type topic struct {
	src string
	dst string
}

func main() {
	flag.Parse()
	log.NewLogger(0, "console", fmt.Sprintf(`{"level": %d, "formatting":false}`, *logLevel))

	if *showVersion {
		fmt.Printf("eventtank (built with %s, git hash %s)\n", runtime.Version(), GitHash)
		return
	}

	if *group == "" {
		log.Fatal(4, "--group is required")
	}

	if *srcTopic == "" {
		log.Fatal(4, "--src-topic is required")
	}

	if *dstTopic == "" {
		log.Fatal(4, "--dst-topic is required")
	}

	if *srcBrokerStr == "" {
		log.Fatal(4, "--src-brokers required")
	}
	if *dstBrokerStr == "" {
		log.Fatal(4, "--dst-brokers required")
	}

	srcBrokers := strings.Split(*srcBrokerStr, ",")
	dstBrokers := strings.Split(*dstBrokerStr, ",")

	consumer, err := NewConsumer(srcBrokers, *group, *srcTopic)
	if err != nil {
		log.Fatal(4, err.Error())
	}
	publisher, err := NewPublisher(dstBrokers, *dstTopic, *compression, *partitionBy)
	if err != nil {
		log.Fatal(4, err.Error())
	}

	log.Info("starting consumer")
	consumer.Start(publisher)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	for {
		select {
		case <-consumer.Done:
			log.Info("consumer ended.")
			break
		case <-sigChan:
			log.Info("shutdown started.")
			consumer.Stop()
		}
	}
	publisher.Stop()

}
