package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"net/http"
	_ "net/http/pprof"

	"github.com/bitly/go-hostpool"
	"github.com/grafana/grafana/pkg/log"
	"github.com/raintank/met"
	"github.com/raintank/met/helper"
	"github.com/nsqio/go-nsq"
	"github.com/raintank/raintank-metric/app"
	"github.com/raintank/raintank-metric/instrumented_nsq"
	"github.com/rakyll/globalconf"
)

var (
	showVersion = flag.Bool("version", false, "print version string")
	dryRun      = flag.Bool("dry", false, "dry run (disable actually storing into kairosdb")

	concurrency  = flag.Int("concurrency", 10, "number of workers parsing messages and writing into kairosdb. also number of nsq consumers for both high and low prio topic")
	topic        = flag.String("topic", "metrics", "NSQ topic")
	topicLowPrio = flag.String("topic-lowprio", "metrics-lowprio", "NSQ topic")
	channel      = flag.String("channel", "kairos", "NSQ channel")
	maxInFlight  = flag.Int("max-in-flight", 200, "max number of messages to allow in flight")

	kairosAddr = flag.String("kairos-addr", "localhost:8080", "kairosdb address (default: localhost:8080)")

	statsdAddr = flag.String("statsd-addr", "localhost:8125", "statsd address (default: localhost:8125)")
	statsdType = flag.String("statsd-type", "standard", "statsd type: standard or datadog (default: standard)")
	confFile   = flag.String("config", "/etc/raintank/nsq_metrics_to_kairos.ini", "configuration file (default /etc/raintank/nsq_metrics_to_kairos.ini")

	consumerOpts     = flag.String("consumer-opt", "", "option to passthrough to nsq.Consumer (may be given multiple times as comma-separated list, http://godoc.org/github.com/nsqio/go-nsq#Config)")
	producerOpts     = flag.String("producer-opt", "", "option to passthrough to nsq.Producer (may be given multiple times as comma-separated list, see http://godoc.org/github.com/nsqio/go-nsq#Config)")
	nsqdTCPAddrs     = flag.String("nsqd-tcp-address", "", "nsqd TCP address (may be given multiple times as comma-separated list)")
	lookupdHTTPAddrs = flag.String("lookupd-http-address", "", "lookupd HTTP address (may be given multiple times as comma-separated list)")
	logLevel = flag.Int("log-level", 2, "log level. 0=TRACE|1=DEBUG|2=INFO|3=WARN|4=ERROR|5=CRITICAL|6=FATAL")
	listenAddr = flag.String("listen", ":6060", "http listener address.")
)

var metricsToKairosOK met.Count
var metricsToKairosFail met.Count
var messagesSize met.Meter
var metricsPerMessage met.Meter
var msgsLowPrioAge met.Meter  // in ms
var msgsHighPrioAge met.Meter // in ms
var kairosPutDuration met.Timer
var inHighPrioItems met.Meter
var inLowPrioItems met.Meter
var msgsToLowPrioOK met.Count
var msgsToLowPrioFail met.Count
var msgsHandleHighPrioOK met.Count
var msgsHandleHighPrioFail met.Count
var msgsHandleLowPrioOK met.Count
var msgsHandleLowPrioFail met.Count

func main() {
	flag.Parse()

	// Only try and parse the conf file if it exists
	if _, err := os.Stat(*confFile); err == nil {
		conf, err := globalconf.NewWithOptions(&globalconf.Options{Filename: *confFile})
		if err != nil {
			log.Fatal(4, err.Error())
		}
		conf.ParseAll()
	}

	log.NewLogger(0, "console", fmt.Sprintf(`{"level": %d, "formatting":true}`, *logLevel))

	if *showVersion {
		fmt.Println("nsq_metrics_to_kairos")
		return
	}
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatal(4, err.Error())
	}
	metrics, err := helper.New(true, *statsdAddr, *statsdType, "nsq_metrics_to_kairos", strings.Replace(hostname, ".", "_", -1))
	if err != nil {
		log.Fatal(4, err.Error())
	}

	if *channel == "" {
		rand.Seed(time.Now().UnixNano())
		*channel = fmt.Sprintf("tail%06d#ephemeral", rand.Int()%999999)
	}

	if *topic == "" {
		log.Fatal(4, "--topic is required")
	}

	if *nsqdTCPAddrs == "" && *lookupdHTTPAddrs == "" {
		log.Fatal(4, "--nsqd-tcp-address or --lookupd-http-address required")
	}
	if *nsqdTCPAddrs != "" && *lookupdHTTPAddrs != "" {
		log.Fatal(4, "use --nsqd-tcp-address or --lookupd-http-address not both")
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	cfg := nsq.NewConfig()
	cfg.UserAgent = "nsq_metrics_to_kairos"
	err = app.ParseOpts(cfg, *consumerOpts)
	if err != nil {
		log.Fatal(4, err.Error())
	}
	cfg.MaxInFlight = *maxInFlight

	consumer, err := insq.NewConsumer(*topic, *channel, cfg, "high_prio.%s", metrics)
	if err != nil {
		log.Fatal(4, err.Error())
	}

	consumerLowPrio, err := insq.NewConsumer(*topicLowPrio, *channel, cfg, "low_prio.%s", metrics)
	if err != nil {
		log.Fatal(4, err.Error())
	}

	pCfg := nsq.NewConfig()
	pCfg.UserAgent = "nsq_metrics_to_kairos"
	err = app.ParseOpts(pCfg, *producerOpts)
	if err != nil {
		log.Fatal(4, err.Error())
	}

	metricsToKairosOK = metrics.NewCount("metrics_to_kairos.ok")
	metricsToKairosFail = metrics.NewCount("metrics_to_kairos.fail")
	messagesSize = metrics.NewMeter("message_size", 0)
	metricsPerMessage = metrics.NewMeter("metrics_per_message", 0)
	msgsLowPrioAge = metrics.NewMeter("low_prio.message_age", 0)
	msgsHighPrioAge = metrics.NewMeter("high_prio.message_age", 0)
	kairosPutDuration = metrics.NewTimer("kairos_put_duration", 0)
	inHighPrioItems = metrics.NewMeter("in_high_prio.items", 0)
	inLowPrioItems = metrics.NewMeter("in_low_prio.items", 0)
	msgsToLowPrioOK = metrics.NewCount("to_kairos.to_low_prio.ok")
	msgsToLowPrioFail = metrics.NewCount("to_low_prio.fail")
	msgsHandleHighPrioOK = metrics.NewCount("handle_high_prio.ok")
	msgsHandleHighPrioFail = metrics.NewCount("handle_high_prio.fail")
	msgsHandleLowPrioOK = metrics.NewCount("handle_low_prio.ok")
	msgsHandleLowPrioFail = metrics.NewCount("handle_low_prio.fail")

	hostPool := hostpool.NewEpsilonGreedy(strings.Split(*nsqdTCPAddrs, ","), 0, &hostpool.LinearEpsilonValueCalculator{})
	producers := make(map[string]*nsq.Producer)
	for _, addr := range strings.Split(*nsqdTCPAddrs, ",") {
		producer, err := nsq.NewProducer(addr, pCfg)
		if err != nil {
			log.Fatal(4, "failed creating producer %s", err.Error())
		}
		producers[addr] = producer
	}

	gateway, err := NewKairosGateway("http://"+*kairosAddr, *dryRun, *concurrency)
	if err != nil {
		log.Fatal(4, err.Error())
	}

	handler := NewKairosHandler(gateway, hostPool, producers)
	consumer.AddConcurrentHandlers(handler, *concurrency)

	handlerLowPrio := NewKairosLowPrioHandler(gateway)
	consumerLowPrio.AddConcurrentHandlers(handlerLowPrio, *concurrency)

	nsqdAdds := strings.Split(*nsqdTCPAddrs, ",")
	if len(nsqdAdds) == 1 && nsqdAdds[0] == "" {
		nsqdAdds = []string{}
	}
	err = consumer.ConnectToNSQDs(nsqdAdds)
	if err != nil {
		log.Fatal(4, err.Error())
	}
	log.Info("connected to nsqd")

	lookupdAdds := strings.Split(*lookupdHTTPAddrs, ",")
	if len(lookupdAdds) == 1 && lookupdAdds[0] == "" {
		lookupdAdds = []string{}
	}
	err = consumer.ConnectToNSQLookupds(lookupdAdds)
	if err != nil {
		log.Fatal(4, err.Error())
	}

	err = consumerLowPrio.ConnectToNSQDs(nsqdAdds)
	if err != nil {
		log.Fatal(4, err.Error())
	}
	log.Info("connected to nsqd")

	err = consumerLowPrio.ConnectToNSQLookupds(lookupdAdds)
	if err != nil {
		log.Fatal(4, err.Error())
	}

	go func() {
		log.Info("starting listener for http/debug on %s", *listenAddr)
		log.Info("%s", http.ListenAndServe(*listenAddr, nil))
	}()

	for {
		select {
		case <-consumer.StopChan:
			consumerLowPrio.Stop()
			return
		case <-consumerLowPrio.StopChan:
			consumer.Stop()
			return
		case <-sigChan:
			consumer.Stop()
			consumerLowPrio.Stop()
		}
	}
}
