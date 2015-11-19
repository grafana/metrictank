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

	"github.com/raintank/raintank-metric/eventdef"
	"github.com/raintank/raintank-metric/schema"
	"github.com/rakyll/globalconf"
)

var (
	showVersion = flag.Bool("version", false, "print version string")

	topic       = flag.String("topic", "probe_events", "NSQ topic")
	channel     = flag.String("channel", "elasticsearch", "NSQ channel")
	maxInFlight = flag.Int("max-in-flight", 200, "max number of messages to allow in flight")

	esAddr = flag.String("elastic-addr", "localhost:9200", "elasticsearch address (default: localhost:9200)")

	statsdAddr = flag.String("statsd-addr", "localhost:8125", "statsd address (default: localhost:8125)")
	statsdType = flag.String("statsd-type", "standard", "statsd type: standard or datadog (default: standard)")
	confFile   = flag.String("config", "/etc/raintank/nsq_probe_events_to_elasticsearch.ini", "configuration file (default /etc/raintank/nsq_probe_events_to_elasticsearch.ini")

	consumerOpts     = flag.String("consumer-opt", "", "option to passthrough to nsq.Consumer (may be given multiple times as comma-separated list, http://godoc.org/github.com/nsqio/go-nsq#Config)")
	nsqdTCPAddrs     = flag.String("nsqd-tcp-address", "", "nsqd TCP address (may be given multiple times as comma-separated list)")
	lookupdHTTPAddrs = flag.String("lookupd-http-address", "", "lookupd HTTP address (may be given multiple times as comma-separated list)")

	eventsToEsOK   met.Count
	eventsToEsFail met.Count
	messagesSize   met.Meter
	msgsAge        met.Meter // in ms
	esPutDuration  met.Timer
	msgsHandleOK   met.Count
	msgsHandleFail met.Count
)

type ESHandler struct {
}

func NewESHandler() (*ESHandler, error) {

	err := eventdef.InitElasticsearch(*esAddr, "", "")
	if err != nil {
		return nil, err
	}

	return &ESHandler{}, nil
}

func (k *ESHandler) HandleMessage(m *nsq.Message) error {
	log.Printf("received message.")
	format := "unknown"
	if m.Body[0] == '\x00' {
		format = "msgFormatJson"
	}
	
	var id int64
	buf := bytes.NewReader(m.Body[1:9])
	binary.Read(buf, binary.BigEndian, &id)
	produced := time.Unix(0, id)

	msgsAge.Value(time.Now().Sub(produced).Nanoseconds() / 1000)
	messagesSize.Value(int64(len(m.Body)))

	event := new(schema.ProbeEvent)
	if err := json.Unmarshal(m.Body[9:], &event); err != nil {
		log.Printf("ERROR: failure to unmarshal message body via format %s: %s. skipping message", format, err)
		return nil
	}

	// Since these messages are being batched, we'll need to hold onto this
	// and ack or requeue it on our own
	m.DisableAutoResponse()

	done := make(chan error, 1)

	// This notifies this function whether or not saving the event worked
	status := make(chan *eventdef.BulkSaveStatus, 1)
	go func() {
		pre := time.Now()
		if err := eventdef.Save(event, status); err != nil {
			fmt.Printf("ERROR: couldn't process %s: %s\n", event.Id, err)
			eventsToEsFail.Inc(1)
			done <- err
			return
		}
		esPutDuration.Value(time.Now().Sub(pre))
		eventsToEsOK.Inc(1)
		done <- nil
	}()

	if err := <-done; err != nil {
		// If saving the event failed, requeue with the default backoff
		// and try again.
		m.Requeue(-1)
		msgsHandleFail.Inc(1)
		return err
	}
	estat := <- status
	if estat.Requeue {
		m.Requeue(-1)
		msgsHandleFail.Inc(1)
		err := fmt.Errorf("event %s failed to save, requeueing", estat.Id)
		log.Println(err)
		return err
	}

	m.Finish()
	msgsHandleOK.Inc(1)

	return nil
}

func main() {
	flag.Parse()

	// Only try and parse the conf file if it exists
	if _, err := os.Stat(*confFile); err == nil {
		conf, err := globalconf.NewWithOptions(&globalconf.Options{Filename: *confFile})
		if err != nil {
			log.Fatal(err)
			os.Exit(1)
		}
		conf.ParseAll()
	}

	if *showVersion {
		fmt.Println("nsq_probe_events_to_elasticsearch")
		return
	}

	if *channel == "" {
		rand.Seed(time.Now().UnixNano())
		*channel = fmt.Sprintf("tail%06d#ephemeral", rand.Int()%999999)
	}

	if *topic == "" {
		log.Fatal("--topic is required")
	}

	if *nsqdTCPAddrs == "" && *lookupdHTTPAddrs == "" {
		log.Fatal(0, "--nsqd-tcp-address or --lookupd-http-address required")
	}
	if *nsqdTCPAddrs != "" && *lookupdHTTPAddrs != "" {
		log.Fatal(0, "use --nsqd-tcp-address or --lookupd-http-address not both")
	}
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatal(err)
	}
	metrics, err := helper.New(true, *statsdAddr, *statsdType, "nsq_probe_events_to_elasticsearch", strings.Replace(hostname, ".", "_", -1))
	if err != nil {
		log.Fatal(err)
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	eventsToEsOK = metrics.NewCount("events_to_es.ok")
	eventsToEsFail = metrics.NewCount("events_to_es.fail")
	messagesSize = metrics.NewMeter("message_size", 0)
	msgsAge = metrics.NewMeter("message_age", 0)
	esPutDuration = metrics.NewTimer("es_put_duration", 0)
	msgsHandleOK = metrics.NewCount("handle.ok")
	msgsHandleFail = metrics.NewCount("handle.fail")

	cfg := nsq.NewConfig()
	cfg.UserAgent = "nsq_probe_events_to_elasticsearch"
	err = app.ParseOpts(cfg, *consumerOpts)
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

	nsqdAdds := strings.Split(*nsqdTCPAddrs, ",")
	if len(nsqdAdds) == 1 && nsqdAdds[0] == "" {
		nsqdAdds = []string{}
	}
	err = consumer.ConnectToNSQDs(nsqdAdds)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("connected to nsqd")

	lookupdAdds := strings.Split(*lookupdHTTPAddrs, ",")
	if len(lookupdAdds) == 1 && lookupdAdds[0] == "" {
		lookupdAdds = []string{}
	}
	err = consumer.ConnectToNSQLookupds(lookupdAdds)
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
			eventdef.StopBulkIndexer()
		}
	}
}
