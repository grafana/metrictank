package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/grafana/grafana/pkg/log"
	met "github.com/grafana/grafana/pkg/metric"
	"github.com/grafana/grafana/pkg/metric/helper"
	"github.com/nsqio/go-nsq"
	"github.com/raintank/raintank-metric/app"
	"github.com/raintank/raintank-metric/instrumented_nsq"

	"github.com/codeskyblue/go-uuid"
	"github.com/raintank/raintank-metric/eventdef"
	"github.com/raintank/raintank-metric/schema"
	"github.com/rakyll/globalconf"
)

var (
	showVersion = flag.Bool("version", false, "print version string")

	topic       = flag.String("topic", "probe_events", "NSQ topic")
	channel     = flag.String("channel", "elasticsearch", "NSQ channel")
	maxInFlight = flag.Int("max-in-flight", 200, "max number of messages to allow in flight")

	concurrency = flag.Int("concurrency", 10, "number of workers parsing messages")

	esAddr = flag.String("elastic-addr", "localhost:9200", "elasticsearch address (default: localhost:9200)")

	statsdAddr = flag.String("statsd-addr", "localhost:8125", "statsd address (default: localhost:8125)")
	statsdType = flag.String("statsd-type", "standard", "statsd type: standard or datadog (default: standard)")
	confFile   = flag.String("config", "/etc/raintank/nsq_probe_events_to_elasticsearch.ini", "configuration file (default /etc/raintank/nsq_probe_events_to_elasticsearch.ini")

	consumerOpts     = flag.String("consumer-opt", "", "option to passthrough to nsq.Consumer (may be given multiple times as comma-separated list, http://godoc.org/github.com/nsqio/go-nsq#Config)")
	nsqdTCPAddrs     = flag.String("nsqd-tcp-address", "", "nsqd TCP address (may be given multiple times as comma-separated list)")
	lookupdHTTPAddrs = flag.String("lookupd-http-address", "", "lookupd HTTP address (may be given multiple times as comma-separated list)")
	logLevel         = flag.Int("log-level", 2, "log level. 0=TRACE|1=DEBUG|2=INFO|3=WARN|4=ERROR|5=CRITICAL|6=FATAL")
	listenAddr       = flag.String("listen", ":6060", "http listener address.")

	eventsToEsOK   met.Count
	eventsToEsFail met.Count
	esPutDuration  met.Timer
	messagesSize   met.Meter
	msgsAge        met.Meter // in ms
	msgsHandleOK   met.Count
	msgsHandleFail met.Count

	writeQueue *InProgressMessageQueue
)

type ESHandler struct {
}

func NewESHandler() (*ESHandler, error) {
	return &ESHandler{}, nil
}

func (k *ESHandler) HandleMessage(m *nsq.Message) error {
	log.Debug("received message.")
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
		log.Error(3, "ERROR: failure to unmarshal message body via format %s: %s. skipping message", format, err)
		return nil
	}

	// Since these messages are being batched, we'll need to hold onto this
	// and ack or requeue it on our own
	m.DisableAutoResponse()
	if event.Id == "" {
		// per http://blog.mikemccandless.com/2014/05/choosing-fast-unique-identifier-uuid.html,
		// using V1 UUIDs is much faster than v4 like we were using
		u := uuid.NewUUID()
		event.Id = u.String()
	}
	writeQueue.EnQueue(event.Id, m)

	if err := eventdef.Save(event); err != nil {
		log.Error(3, "couldn't process %s: %s", event.Id, err)
		msgsHandleFail.Inc(1)
		m.Requeue(-1)
		return err
	}

	return nil
}

type inProgressMessage struct {
	timestamp time.Time
	message   *nsq.Message
}

type InProgressMessageQueue struct {
	sync.RWMutex
	inProgress map[string]*inProgressMessage
	status     chan *eventdef.BulkSaveStatus
}

func (q *InProgressMessageQueue) EnQueue(id string, m *nsq.Message) {
	q.Lock()
	q.inProgress[id] = &inProgressMessage{
		timestamp: time.Now(),
		message:   m,
	}
	q.Unlock()
}

func (q *InProgressMessageQueue) loop() {
	for {
		select {
		case s := <-q.status:
			q.Lock()
			if m, ok := q.inProgress[s.Id]; ok {
				if s.Ok {
					if m.message != nil {
						m.message.Finish()
					}
					eventsToEsOK.Inc(1)
					msgsHandleOK.Inc(1)
					log.Debug("event %s commited to ES", s.Id)
				} else {
					if m.message != nil {
						m.message.Requeue(-1)
					}
					eventsToEsFail.Inc(1)
					msgsHandleFail.Inc(1)
					log.Error(3, "event %s failed to save, requeueing", s.Id)
				}
				esPutDuration.Value(time.Now().Sub(m.timestamp))
			} else {
				log.Error(3, "got processing response for unknown message. event %s", s.Id)
			}
			delete(q.inProgress, s.Id)
			q.Unlock()
		}
	}
}

func NewInProgressMessageQueue() *InProgressMessageQueue {
	q := &InProgressMessageQueue{
		inProgress: make(map[string]*inProgressMessage),
		status:     make(chan *eventdef.BulkSaveStatus, *maxInFlight),
	}
	for i := 0; i < *concurrency; i++ {
		go q.loop()
	}
	return q
}

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
		fmt.Println("nsq_probe_events_to_elasticsearch")
		return
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

	hostname, err := os.Hostname()
	if err != nil {
		log.Fatal(4, err.Error())
	}
	metrics, err := helper.New(true, *statsdAddr, *statsdType, "nsq_probe_events_to_elasticsearch", strings.Replace(hostname, ".", "_", -1))
	if err != nil {
		log.Fatal(4, err.Error())
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	messagesSize = metrics.NewMeter("message_size", 0)
	msgsAge = metrics.NewMeter("message_age", 0)
	eventsToEsOK = metrics.NewCount("events_to_es.ok")
	eventsToEsFail = metrics.NewCount("events_to_es.fail")
	esPutDuration = metrics.NewTimer("es_put_duration", 0)
	msgsHandleOK = metrics.NewCount("handle.ok")
	msgsHandleFail = metrics.NewCount("handle.fail")

	writeQueue = NewInProgressMessageQueue()

	err = eventdef.InitElasticsearch(*esAddr, "", "", writeQueue.status, *maxInFlight)
	if err != nil {
		log.Fatal(4, err.Error())
	}

	cfg := nsq.NewConfig()
	cfg.UserAgent = "nsq_probe_events_to_elasticsearch"
	err = app.ParseOpts(cfg, *consumerOpts)
	if err != nil {
		log.Fatal(4, err.Error())
	}
	cfg.MaxInFlight = *maxInFlight

	consumer, err := insq.NewConsumer(*topic, *channel, cfg, "%s", metrics)

	if err != nil {
		log.Fatal(4, err.Error())
	}

	handler, err := NewESHandler()
	if err != nil {
		log.Fatal(4, err.Error())
	}

	consumer.AddConcurrentHandlers(handler, *concurrency)

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

	go func() {
		log.Info("INFO starting listener for http/debug on %s", *listenAddr)
		httperr := http.ListenAndServe(*listenAddr, nil)
		if httperr != nil {
			log.Info(httperr.Error())
		}
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
