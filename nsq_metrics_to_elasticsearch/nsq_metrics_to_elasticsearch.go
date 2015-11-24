package main

import (
	"flag"
	"fmt"
	"math/rand"
	"net/http"
	_ "net/http/pprof"

	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/grafana/grafana/pkg/log"
	met "github.com/grafana/grafana/pkg/metric"
	"github.com/grafana/grafana/pkg/metric/helper"
	"github.com/nsqio/go-nsq"
	"github.com/raintank/raintank-metric/app"
	"github.com/raintank/raintank-metric/instrumented_nsq"
	"github.com/raintank/raintank-metric/metricdef"
	"github.com/raintank/raintank-metric/msg"
	"github.com/rakyll/globalconf"
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
	confFile   = flag.String("config", "/etc/raintank/nsq_metrics_to_elasticsearch.ini", "configuration file (default /etc/raintank/nsq_metrics_to_elasticsearch.ini")

	consumerOpts     = flag.String("consumer-opt", "", "option to passthrough to nsq.Consumer (may be given multiple times as comma-separated list, http://godoc.org/github.com/nsqio/go-nsq#Config)")
	nsqdTCPAddrs     = flag.String("nsqd-tcp-address", "", "nsqd TCP address (may be given multiple times as comma-separated list)")
	lookupdHTTPAddrs = flag.String("lookupd-http-address", "", "lookupd HTTP address (may be given multiple times as comma-separated list)")

	logLevel = flag.Int("log-level", 2, "log level. 0=TRACE|1=DEBUG|2=INFO|3=WARN|4=ERROR|5=CRITICAL|6=FATAL")
	listenAddr = flag.String("listen", ":6060", "http listener address.")

	metricsToEsOK     met.Count
	metricsToEsFail   met.Count
	messagesSize      met.Meter
	metricsPerMessage met.Meter
	msgsAge           met.Meter // in ms
	esPutDuration     met.Timer
	msgsHandleOK      met.Count
	msgsHandleFail    met.Count
)

type ESHandler struct {
}

func NewESHandler() (*ESHandler, error) {

	return &ESHandler{}, nil
}

func (k *ESHandler) HandleMessage(m *nsq.Message) error {
	messagesSize.Value(int64(len(m.Body)))

	ms, err := msg.MetricDataFromMsg(m.Body)
	if err != nil {
		log.Error(3, "failed to get metric from message, skipping. %s", err)
		return nil
	}
	msgsAge.Value(time.Now().Sub(ms.Produced).Nanoseconds() / 1000)

	err = ms.DecodeMetricData()
	if err != nil {
		log.Error(3, "failed to decode message body, skipping. %s", err)
		return nil
	}
	metricsPerMessage.Value(int64(len(ms.Metrics)))

	done := make(chan error, 1)
	go func() {
		pre := time.Now()
		for i, m := range ms.Metrics {
			if err := metricdef.EnsureIndex(m); err != nil {
				log.Error(3, "couldn't process %s: %s", m.Id(), err)
				metricsToEsFail.Inc(int64(len(ms.Metrics) - i))
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
	metricsToEsOK.Inc(int64(len(ms.Metrics)))

	msgsHandleOK.Inc(1)
	return nil
}

func main() {
	flag.Parse()

	// Only try and parse the conf file if it exists
	if _, err := os.Stat(*confFile); err == nil {
		conf, err := globalconf.NewWithOptions(&globalconf.Options{Filename: *confFile})
		if err != nil {
			log.Fatal(3, "Could not parse config file. %s", err)
		}
		conf.ParseAll()
	}

	if *showVersion {
		fmt.Println("nsq_metrics_to_elasticsearch")
		return
	}

	//initialize logger.
	log.NewLogger(0, "console", fmt.Sprintf(`{"level": %d, "formatting":true}`, *logLevel))
	log.Debug("debug log.")
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

	hostname, err := os.Hostname()
	if err != nil {
		log.Fatal(4, "could not resolve hostname. %s", err)
	}
	metrics, err := helper.New(true, *statsdAddr, *statsdType, "nsq_metrics_to_elasticsearch", strings.Replace(hostname, ".", "_", -1))
	if err != nil {
		log.Fatal(4, "failed to initialize statsd. %s", err)
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

	err = metricdef.InitElasticsearch(*esAddr, "", "")
	if err != nil {
		log.Fatal(4, "failed to initialize Elasticsearch. %s", err)
	}
	err = metricdef.InitRedis(*redisAddr, "", "")
	if err != nil {
		log.Fatal(4, "failed to initialize redis. %s", err)
	}

	cfg := nsq.NewConfig()
	cfg.UserAgent = "nsq_metrics_to_elasticsearch"
	err = app.ParseOpts(cfg, *consumerOpts)
	if err != nil {
		log.Fatal(4, "failed to parse consumer opts. %s", err)
	}
	cfg.MaxInFlight = *maxInFlight

	consumer, err := insq.NewConsumer(*topic, *channel, cfg, "%s", metrics)
	if err != nil {
		log.Fatal(4, "failed to create nsq consumer. %s", err)
	}

	handler, err := NewESHandler()
	if err != nil {
		log.Fatal(4, "failed to create handler. %s", err)
	}

	consumer.AddConcurrentHandlers(handler, 80)

	nsqdAdds := strings.Split(*nsqdTCPAddrs, ",")
	if len(nsqdAdds) == 1 && nsqdAdds[0] == "" {
		nsqdAdds = []string{}
	}
	err = consumer.ConnectToNSQDs(nsqdAdds)
	if err != nil {
		log.Fatal(4, "failed to connect to nsqd. %s", err)
	}
	log.Info("connected to nsqd")

	lookupdAdds := strings.Split(*lookupdHTTPAddrs, ",")
	if len(lookupdAdds) == 1 && lookupdAdds[0] == "" {
		lookupdAdds = []string{}
	}
	err = consumer.ConnectToNSQLookupds(lookupdAdds)
	if err != nil {
		log.Fatal(4, "failed to connect to nsqlookupd. %s", err)
	}
	go func() {
		log.Info("INFO starting listener for http/debug on %s", *listenAddr)
		log.Info("%s", http.ListenAndServe(*listenAddr, nil))
	}()

	for {
		select {
		case <-consumer.StopChan:
			//flush elastic BulkAPi buffer
			metricdef.Indexer.Stop()
			return
		case <-sigChan:
			consumer.Stop()
			<-consumer.StopChan
		}
	}
}
