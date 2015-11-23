package main

import (
	"flag"
	"fmt"
	"math/rand"
	"net/http"
	_ "net/http/pprof"
	"strings"

	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/grafana/grafana/pkg/log"
	"github.com/nsqio/go-nsq"
	"github.com/raintank/raintank-metric/app"
	"github.com/raintank/raintank-metric/msg"
)

var (
	showVersion = flag.Bool("version", false, "print version string")

	topic       = flag.String("topic", "metrics", "NSQ topic")
	channel     = flag.String("channel", "stdout<random-number>#ephemeral", "NSQ channel")
	maxInFlight = flag.Int("max-in-flight", 200, "max number of messages to allow in flight")

	consumerOpts     = flag.String("consumer-opt", "", "option to passthrough to nsq.Consumer (may be given multiple times as comma-separated list, http://godoc.org/github.com/nsqio/go-nsq#Config)")
	nsqdTCPAddrs     = flag.String("nsqd-tcp-address", "", "nsqd TCP address (may be given multiple times as comma-separated list)")
	lookupdHTTPAddrs = flag.String("lookupd-http-address", "", "lookupd HTTP address (may be given multiple times as comma-separated list)")
	logLevel = flag.Int("log-level", 2, "log level. 0=TRACE|1=DEBUG|2=INFO|3=WARN|4=ERROR|5=CRITICAL|6=FATAL")
	listenAddr = flag.String("listen", ":6060", "http listener address.")
)

type StdoutHandler struct {
}

func NewStdoutHandler() (*StdoutHandler, error) {

	return &StdoutHandler{}, nil
}

func (k *StdoutHandler) HandleMessage(m *nsq.Message) error {
	ms, err := msg.MetricDataFromMsg(m.Body)
	if err != nil {
		log.Error(0, "%s: skipping message", err.Error())
		return nil
	}

	err = ms.DecodeMetricData()
	if err != nil {
		log.Error(0, "%s: skipping message", err.Error())
		return nil
	}

	for _, m := range ms.Metrics {
		fmt.Println(m.Name, m.Time, m.Value, m.Tags)
	}
	return nil
}

func main() {
	flag.Parse()

	log.NewLogger(0, "console", fmt.Sprintf(`{"level": %d, "formatting":true}`, *logLevel))

	if *showVersion {
		fmt.Println("nsq_metrics_to_stdout")
		return
	}

	if *channel == "" || *channel == "stdout<random-number>#ephemeral" {
		rand.Seed(time.Now().UnixNano())
		*channel = fmt.Sprintf("stdout%06d#ephemeral", rand.Int()%999999)
	}

	if *topic == "" {
		log.Fatal(0, "--topic is required")
	}

	if *nsqdTCPAddrs == "" && *lookupdHTTPAddrs == "" {
		log.Fatal(0, "--nsqd-tcp-address or --lookupd-http-address required")
	}
	if *nsqdTCPAddrs != "" && *lookupdHTTPAddrs != "" {
		log.Fatal(0, "use --nsqd-tcp-address or --lookupd-http-address not both")
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	cfg := nsq.NewConfig()
	cfg.UserAgent = "nsq_metrics_to_stdout"
	err := app.ParseOpts(cfg, *consumerOpts)
	if err != nil {
		log.Fatal(0, err.Error())
	}
	cfg.MaxInFlight = *maxInFlight

	consumer, err := nsq.NewConsumer(*topic, *channel, cfg)
	if err != nil {
		log.Fatal(0, err.Error())
	}

	handler, err := NewStdoutHandler()
	if err != nil {
		log.Fatal(0, err.Error())
	}

	consumer.AddHandler(handler)

	nsqdAdds := strings.Split(*nsqdTCPAddrs, ",")
	if len(nsqdAdds) == 1 && nsqdAdds[0] == "" {
		nsqdAdds = []string{}
	}
	err = consumer.ConnectToNSQDs(nsqdAdds)
	if err != nil {
		log.Fatal(0, err.Error())
	}
	log.Info("connected to nsqd")

	lookupdAdds := strings.Split(*lookupdHTTPAddrs, ",")
	if len(lookupdAdds) == 1 && lookupdAdds[0] == "" {
		lookupdAdds = []string{}
	}
	err = consumer.ConnectToNSQLookupds(lookupdAdds)
	if err != nil {
		log.Fatal(0, err.Error())
	}
	go func() {
		log.Info("starting listener for http/debug on %s", *listenAddr)
		log.Info("%s", http.ListenAndServe(*listenAddr, nil))
	}()

	for {
		select {
		case <-consumer.StopChan:
			return
		case <-sigChan:
			consumer.Stop()
			<-consumer.StopChan
		}
	}
}
