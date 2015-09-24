package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"net/http"
	_ "net/http/pprof"

	met "github.com/grafana/grafana/pkg/metric"
	"github.com/grafana/grafana/pkg/metric/helper"
	"github.com/nsqio/go-nsq"
	"github.com/raintank/raintank-metric/app"
	"github.com/raintank/raintank-metric/instrumented_nsq"
)

var (
	showVersion = flag.Bool("version", false, "print version string")
	dryRun      = flag.Bool("dry", false, "dry run (disable actually storing into kairosdb")

	// TODO split up for consumer and cassandra sender
	concurrency = flag.Int("concurrency", 10, "number of workers parsing messages and writing into kairosdb. also number of nsq consumers for both high and low prio topic")
	topic       = flag.String("topic", "metrics", "NSQ topic")
	channel     = flag.String("channel", "tank", "NSQ channel")
	maxInFlight = flag.Int("max-in-flight", 200, "max number of messages to allow in flight")

	kairosAddr = flag.String("kairos-addr", "localhost:8080", "kairosdb address (default: localhost:8080)")

	statsdAddr = flag.String("statsd-addr", "localhost:8125", "statsd address (default: localhost:8125)")
	statsdType = flag.String("statsd-type", "standard", "statsd type: standard or datadog (default: standard)")

	consumerOpts     = app.StringArray{}
	producerOpts     = app.StringArray{}
	nsqdTCPAddrs     = app.StringArray{}
	lookupdHTTPAddrs = app.StringArray{}

	metrics *AggMetrics
)

func init() {
	flag.Var(&consumerOpts, "consumer-opt", "option to passthrough to nsq.Consumer (may be given multiple times, http://godoc.org/github.com/nsqio/go-nsq#Config)")
	flag.Var(&producerOpts, "producer-opt", "option to passthrough to nsq.Producer (may be given multiple times, see http://godoc.org/github.com/nsqio/go-nsq#Config)")

	flag.Var(&nsqdTCPAddrs, "nsqd-tcp-address", "nsqd TCP address (may be given multiple times)")
	flag.Var(&lookupdHTTPAddrs, "lookupd-http-address", "lookupd HTTP address (may be given multiple times)")
}

var metricsToKairosOK met.Count
var metricsToKairosFail met.Count
var messagesSize met.Meter
var metricsPerMessage met.Meter
var msgsHighPrioAge met.Meter // in ms
var kairosPutDuration met.Timer
var inHighPrioItems met.Meter
var msgsHandleHighPrioOK met.Count
var msgsHandleHighPrioFail met.Count

func main() {
	flag.Parse()

	if *showVersion {
		fmt.Println("nsq_metrics_tank")
		return
	}
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatal(err)
	}
	stats, err := helper.New(true, *statsdAddr, *statsdType, "nsq_metrics_tank", strings.Replace(hostname, ".", "_", -1))
	if err != nil {
		log.Fatal(err)
	}

	if *channel == "" {
		rand.Seed(time.Now().UnixNano())
		*channel = fmt.Sprintf("tail%06d#ephemeral", rand.Int()%999999)
	}

	if *topic == "" {
		log.Fatal("--topic is required")
	}

	if len(nsqdTCPAddrs) == 0 && len(lookupdHTTPAddrs) == 0 {
		log.Fatal("--nsqd-tcp-address or --lookupd-http-address required")
	}
	if len(nsqdTCPAddrs) > 0 && len(lookupdHTTPAddrs) > 0 {
		log.Fatal("use --nsqd-tcp-address or --lookupd-http-address not both")
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	cfg := nsq.NewConfig()
	cfg.UserAgent = "nsq_metrics_tank"
	err = app.ParseOpts(cfg, consumerOpts)
	if err != nil {
		log.Fatal(err)
	}
	cfg.MaxInFlight = *maxInFlight

	consumer, err := insq.NewConsumer(*topic, *channel, cfg, "high_prio.%s", stats)
	if err != nil {
		log.Fatal(err)
	}

	pCfg := nsq.NewConfig()
	pCfg.UserAgent = "nsq_metrics_tank"
	err = app.ParseOpts(pCfg, producerOpts)
	if err != nil {
		log.Fatal(err)
	}

	metricsToKairosOK = stats.NewCount("metrics_to_kairos.ok")
	metricsToKairosFail = stats.NewCount("metrics_to_kairos.fail")
	messagesSize = stats.NewMeter("message_size", 0)
	metricsPerMessage = stats.NewMeter("metrics_per_message", 0)
	msgsHighPrioAge = stats.NewMeter("high_prio.message_age", 0)
	kairosPutDuration = stats.NewTimer("kairos_put_duration", 0)
	inHighPrioItems = stats.NewMeter("in_high_prio.items", 0)
	msgsHandleHighPrioOK = stats.NewCount("handle_high_prio.ok")
	msgsHandleHighPrioFail = stats.NewCount("handle_high_prio.fail")

	metrics = NewAggMetrics()
	handler := NewHandler(metrics)
	consumer.AddConcurrentHandlers(handler, *concurrency)

	err = consumer.ConnectToNSQDs(nsqdTCPAddrs)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("INFO : connected to nsqd")

	err = consumer.ConnectToNSQLookupds(lookupdHTTPAddrs)
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		http.HandleFunc("/get", Get)
		log.Println("INFO starting listener for metrics and http/debug on :6060")
		log.Println(http.ListenAndServe(":6060", nil))
	}()

	for {
		select {
		case <-consumer.StopChan:
			consumer.Stop()
			return
		case <-sigChan:
			consumer.Stop()
		}
	}
}
