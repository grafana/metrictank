package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/bitly/go-nsq"
	"github.com/bitly/nsq/internal/app"
	"github.com/raintank/raintank-metric/metricdef"
	"github.com/raintank/raintank-metric/metricstore"
)

var (
	showVersion = flag.Bool("version", false, "print version string")

	topic         = flag.String("topic", "", "NSQ topic")
	channel       = flag.String("channel", "", "NSQ channel")
	maxInFlight   = flag.Int("max-in-flight", 200, "max number of messages to allow in flight")
	totalMessages = flag.Int("n", 0, "total messages to process (will wait if starved)")

	consumerOpts     = app.StringArray{}
	nsqdTCPAddrs     = app.StringArray{}
	lookupdHTTPAddrs = app.StringArray{}
)

func init() {
	flag.Var(&consumerOpts, "consumer-opt", "option to passthrough to nsq.Consumer (may be given multiple times, http://godoc.org/github.com/bitly/go-nsq#Config)")
	flag.Var(&nsqdTCPAddrs, "nsqd-tcp-address", "nsqd TCP address (may be given multiple times)")
	flag.Var(&lookupdHTTPAddrs, "lookupd-http-address", "lookupd HTTP address (may be given multiple times)")
}

type KairosHandler struct {
	totalMessages int
	messagesDone  int
	kairos        *metricstore.Kairosdb
}

func NewKairosHandler(totalMessages int) (*KairosHandler, error) {
	kairos, err := metricstore.NewKairosdb("http://kairosdb:8080")
	if err != nil {
		return nil, err
	}
	return &KairosHandler{
		totalMessages: totalMessages,
		kairos:        kairos,
	}, nil
}

func (k *KairosHandler) HandleMessage(m *nsq.Message) error {
	fmt.Printf("got message: %v", m)
	k.messagesDone++
	metrics := make([]*metricdef.IndvMetric, 0)
	if err := json.Unmarshal(m.Body, &metrics); err != nil {
		log.Printf("ERROR: failure to unmarshal message body: %s. skipping message", err)
		return nil
	}
	err := k.kairos.SendMetricPointers(metrics)
	if err != nil {
		log.Printf("ERROR: can't send to kairosdb: %s. retrying later", err)
		return err
	}

	if k.totalMessages > 0 && k.messagesDone >= k.totalMessages {
		os.Exit(0)
	}
	return nil
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

	// Don't ask for more messages than we want
	if *totalMessages > 0 && *totalMessages < *maxInFlight {
		*maxInFlight = *totalMessages
	}

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

	handler, err := NewKairosHandler(*totalMessages)
	if err != nil {
		log.Fatal(err)
	}

	consumer.AddHandler(handler)

	err = consumer.ConnectToNSQDs(nsqdTCPAddrs)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("connected to nsqd")

	err = consumer.ConnectToNSQLookupds(lookupdHTTPAddrs)
	if err != nil {
		log.Fatal(err)
	}

	for {
		select {
		case <-consumer.StopChan:
			return
		case <-sigChan:
			consumer.Stop()
		}
	}
}
