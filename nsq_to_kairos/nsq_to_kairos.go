package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/bitly/go-hostpool"
	"github.com/bitly/go-nsq"
	"github.com/bitly/nsq/internal/app"
)

var (
	showVersion = flag.Bool("version", false, "print version string")

	topic        = flag.String("topic", "metrics", "NSQ topic")
	topicLowPrio = flag.String("topic-lowprio", "metrics-lowprio", "NSQ topic")
	channel      = flag.String("channel", "", "NSQ channel")
	maxInFlight  = flag.Int("max-in-flight", 200, "max number of messages to allow in flight")

	consumerOpts     = app.StringArray{}
	producerOpts     = app.StringArray{}
	nsqdTCPAddrs     = app.StringArray{}
	lookupdHTTPAddrs = app.StringArray{}
)

func init() {
	flag.Var(&consumerOpts, "consumer-opt", "option to passthrough to nsq.Consumer (may be given multiple times, http://godoc.org/github.com/bitly/go-nsq#Config)")
	flag.Var(&producerOpts, "producer-opt", "option to passthrough to nsq.Producer (may be given multiple times, see http://godoc.org/github.com/bitly/go-nsq#Config)")

	flag.Var(&nsqdTCPAddrs, "nsqd-tcp-address", "nsqd TCP address (may be given multiple times)")
	flag.Var(&lookupdHTTPAddrs, "lookupd-http-address", "lookupd HTTP address (may be given multiple times)")
}

func main() {
	flag.Parse()

	if *showVersion {
		fmt.Println("nsq_to_kairos")
		return
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
	cfg.UserAgent = "nsq_to_kairos"
	err := app.ParseOpts(cfg, consumerOpts)
	if err != nil {
		log.Fatal(err)
	}
	cfg.MaxInFlight = *maxInFlight

	consumer, err := nsq.NewConsumer(*topic, *channel, cfg)
	if err != nil {
		log.Fatal(err)
	}

	consumerLowPrio, err := nsq.NewConsumer(*topicLowPrio, *channel, cfg)
	if err != nil {
		log.Fatal(err)
	}

	pCfg := nsq.NewConfig()
	pCfg.UserAgent = "nsq_to_kairos"
	err = app.ParseOpts(pCfg, producerOpts)
	if err != nil {
		log.Fatal(err)
	}

	hostPool := hostpool.NewEpsilonGreedy(nsqdTCPAddrs, 0, &hostpool.LinearEpsilonValueCalculator{})
	producers := make(map[string]*nsq.Producer)
	for _, addr := range nsqdTCPAddrs {
		producer, err := nsq.NewProducer(addr, pCfg)
		if err != nil {
			log.Fatalf("failed creating producer %s", err)
		}
		producers[addr] = producer
	}

	gateway, err := NewKairosGateway()
	if err != nil {
		log.Fatal(err)
	}

	handler := NewKairosHandler(gateway, hostPool, producers)
	consumer.AddConcurrentHandlers(handler, 4)

	handlerLowPrio := NewKairosLowPrioHandler(gateway)
	consumerLowPrio.AddConcurrentHandlers(handlerLowPrio, 2)

	err = consumer.ConnectToNSQDs(nsqdTCPAddrs)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("connected to nsqd")

	err = consumer.ConnectToNSQLookupds(lookupdHTTPAddrs)
	if err != nil {
		log.Fatal(err)
	}

	err = consumerLowPrio.ConnectToNSQDs(nsqdTCPAddrs)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("connected to nsqd")

	err = consumerLowPrio.ConnectToNSQLookupds(lookupdHTTPAddrs)
	if err != nil {
		log.Fatal(err)
	}

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
