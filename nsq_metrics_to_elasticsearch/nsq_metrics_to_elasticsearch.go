package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	_ "net/http/pprof"

	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	met "github.com/grafana/grafana/pkg/metric"
	"github.com/grafana/grafana/pkg/metric/helper"
	"github.com/nsqio/go-nsq"
	"github.com/raintank/raintank-metric/app"
	"github.com/raintank/raintank-metric/instrumented_nsq"
	"github.com/raintank/raintank-metric/metricdef"
	"github.com/raintank/raintank-metric/schema"
)

var (
	showVersion = flag.Bool("version", false, "print version string")

	topic       = flag.String("topic", "metrics", "NSQ topic")
	channel     = flag.String("channel", "elasticsearch", "NSQ channel")
	maxInFlight = flag.Int("max-in-flight", 200, "max number of messages to allow in flight")

	esAddr    = flag.String("elastic-addr", "localhost:9200", "elasticsearch address (default: localhost:9200)")
	redisAddr = flag.String("redis-addr", "localhost:6379", "redis address (default: localhost:6379)")

	statsdAddr = flag.String("statsd-addr", "localhost:8125", "statsd address (default: localhost:8125)")
	statsdType = flag.String("statsd-type", "standard", "statsd type: standard or datadog (default: standard)")

	consumerOpts     = app.StringArray{}
	nsqdTCPAddrs     = app.StringArray{}
	lookupdHTTPAddrs = app.StringArray{}

	metricsToEsOK     met.Count
	metricsToEsFail   met.Count
	messagesSize      met.Meter
	metricsPerMessage met.Meter
	msgsAge           met.Meter // in ms
	esPutDuration     met.Timer
	msgsHandleOK      met.Count
	msgsHandleFail    met.Count
)

func init() {
	flag.Var(&consumerOpts, "consumer-opt", "option to passthrough to nsq.Consumer (may be given multiple times, http://godoc.org/github.com/nsqio/go-nsq#Config)")
	flag.Var(&nsqdTCPAddrs, "nsqd-tcp-address", "nsqd TCP address (may be given multiple times)")
	flag.Var(&lookupdHTTPAddrs, "lookupd-http-address", "lookupd HTTP address (may be given multiple times)")
}

type ESHandler struct {
}

func NewESHandler() (*ESHandler, error) {
	err := metricdef.InitElasticsearch(*esAddr, "", "")
	if err != nil {
		return nil, err
	}
	err = metricdef.InitRedis(*redisAddr, "", "")
	if err != nil {
		return nil, err
	}

	return &ESHandler{}, nil
}

func (k *ESHandler) HandleMessage(m *nsq.Message) error {
	messagesSize.Value(int64(len(m.Body)))

	var id int64
	buf := bytes.NewReader(m.Body[1:9])
	binary.Read(buf, binary.BigEndian, &id)
	produced := time.Unix(0, id)
	msgsAge.Value(time.Now().Sub(produced).Nanoseconds() / 1000)

	metrics := make([]*schema.MetricData, 0)
	var err error
	format := "unknown"
	if m.Body[0] == '\x00' {
		format = "msgFormatMetricDefinitionArrayJson"
		err = json.Unmarshal(m.Body[9:], &metrics)
	}
	if m.Body[0] == '\x01' {
		format = "msgFormatMetricDataArrayMsgp"
		var out schema.MetricDataArray
		_, err = out.UnmarshalMsg(m.Body[9:])
		metrics = []*schema.MetricData(out)
	}

	if err != nil {
		log.Printf("ERROR: failure to unmarshal message body via format %s: %s. skipping message", format, err)
		return nil
	}
	metricsPerMessage.Value(int64(len(metrics)))

	done := make(chan error, 1)
	go func() {
		pre := time.Now()
		for i, m := range metrics {
			if err := metricdef.EnsureIndex(m); err != nil {
				fmt.Printf("ERROR: couldn't process %s: %s\n", m.Id, err)
				metricsToEsFail.Inc(int64(len(metrics) - i))
				done <- err
				return
			}
		}
		esPutDuration.Value(time.Now().Sub(pre))
		done <- nil
	}()

	if err := <-done; err != nil {
		msgsHandleFail.Inc(1)
		return err
	}
	metricsToEsOK.Inc(int64(len(metrics)))

	msgsHandleOK.Inc(1)
	return nil
}

func main() {
	flag.Parse()

	if *showVersion {
		fmt.Println("nsq_metrics_to_elasticsearch")
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

	hostname, err := os.Hostname()
	if err != nil {
		log.Fatal(err)
	}
	metrics, err := helper.New(true, *statsdAddr, *statsdType, "nsq_metrics_to_elasticsearch", strings.Replace(hostname, ".", "_", -1))
	if err != nil {
		log.Fatal(err)
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	metricsToEsOK = metrics.NewCount("metrics_to_es.ok")
	metricsToEsFail = metrics.NewCount("metrics_to_es.fail")
	messagesSize = metrics.NewMeter("message_size", 0)
	metricsPerMessage = metrics.NewMeter("metrics_per_message", 0)
	msgsAge = metrics.NewMeter("message_age", 0)
	esPutDuration = metrics.NewTimer("es_put_duration", 0)
	msgsHandleOK = metrics.NewCount("handle.ok")
	msgsHandleFail = metrics.NewCount("handle.fail")

	cfg := nsq.NewConfig()
	cfg.UserAgent = "nsq_metrics_to_elasticsearch"
	err = app.ParseOpts(cfg, consumerOpts)
	if err != nil {
		log.Fatal(err)
	}
	cfg.MaxInFlight = *maxInFlight

	consumer, err := insq.NewConsumer(*topic, *channel, cfg, "%s", metrics)
	if err != nil {
		log.Fatal(err)
	}

	handler, err := NewESHandler()
	if err != nil {
		log.Fatal(err)
	}

	consumer.AddConcurrentHandlers(handler, 80)

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
