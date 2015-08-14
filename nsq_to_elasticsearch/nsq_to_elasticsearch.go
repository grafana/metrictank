package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	_ "net/http/pprof"

	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/bitly/go-nsq"
	"github.com/bitly/nsq/internal/app"
	"github.com/raintank/raintank-metric/metricdef"
	"github.com/raintank/raintank-metric/setting"
)

var metricDefs *metricdef.MetricDefCache

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

type ESHandler struct {
	totalMessages int
	messagesDone  int
}

func NewESHandler(totalMessages int) (*ESHandler, error) {

	err := metricdef.InitElasticsearch()
	if err != nil {
		return nil, err
	}
	err = metricdef.InitRedis()
	if err != nil {
		return nil, err
	}

	metricDefs, err = metricdef.InitMetricDefCache()
	if err != nil {
		return nil, err
	}

	return &ESHandler{
		totalMessages: totalMessages,
	}, nil
}

func (k *ESHandler) HandleMessage(m *nsq.Message) error {
	k.messagesDone++
	format := "unknown"
	if m.Body[0] == '\x00' {
		format = "msgFormatMetricDefinitionArrayJson"
	}

	metrics := make([]*metricdef.IndvMetric, 0)
	if err := json.Unmarshal(m.Body[9:], &metrics); err != nil {
		log.Printf("ERROR: failure to unmarshal message body via format %s: %s. skipping message", format, err)
		return nil
	}

	for _, m := range metrics {
		id := fmt.Sprintf("%d.%s", m.OrgId, m.Name)
		if m.Id == "" {
			m.Id = id
		}
		if err := metricDefs.CheckMetricDef(id, m); err != nil {
			fmt.Printf("ERROR: couldn't process %s: %s\n", id, err)
			return err
		}
	}

	if k.totalMessages > 0 && k.messagesDone >= k.totalMessages {
		os.Exit(0)
	}
	return nil
}

func main() {
	flag.Parse()

	if *showVersion {
		fmt.Println("nsq_to_elasticsearch")
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

	setting.Config = new(setting.Conf)
	setting.Config.ElasticsearchDomain = "elasticsearch"
	setting.Config.ElasticsearchPort = 9200
	setting.Config.RedisAddr = "redis:6379"

	cfg := nsq.NewConfig()
	cfg.UserAgent = "nsq_to_elasticsearch"
	err := app.ParseOpts(cfg, consumerOpts)
	if err != nil {
		log.Fatal(err)
	}
	cfg.MaxInFlight = *maxInFlight

	consumer, err := nsq.NewConsumer(*topic, *channel, cfg)
	if err != nil {
		log.Fatal(err)
	}

	handler, err := NewESHandler(*totalMessages)
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
	go func() {
		log.Println("INFO starting listener for http/debug on :6060")
		log.Println(http.ListenAndServe(":6060", nil))
	}()

	for {
		select {
		case <-consumer.StopChan:
			return
		case <-sigChan:
			consumer.Stop()
		}
	}
}
