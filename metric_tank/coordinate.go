package main

import (
	"encoding/json"
	"fmt"

	"github.com/bitly/go-hostpool"
	"github.com/grafana/grafana/pkg/log"
	met "github.com/grafana/grafana/pkg/metric"
	"github.com/nsqio/go-nsq"
	"github.com/raintank/raintank-metric/instrumented_nsq"
)

var (
	hostPool  hostpool.HostPool
	producers map[string]*nsq.Producer
)

type PersistMessage struct {
	Instance string `json:"instance"`
	Key      string `json:"key"`
	T0       uint32 `json:"t0"`
}

func (msg *PersistMessage) Send() {
	if *topicNotifyPersist == "" {
		return
	}
	body, err := json.Marshal(&msg)
	if err != nil {
		log.Fatal(4, "failed to marshal persistMessage to json.")
	}
	hostPoolResponse := hostPool.Get()
	p := producers[hostPoolResponse.Host()]
	err = p.Publish(*topicNotifyPersist, body)
	if err != nil {
		log.Warn("publisher marking host %s as faulty due to %s", hostPoolResponse.Host(), err)
		hostPoolResponse.Mark(err)
		msg.Send()
	}
	return
}

type MetricPersistHandler struct {
	metrics Metrics
}

func NewMetricPersistHandler(metrics Metrics) *MetricPersistHandler {
	return &MetricPersistHandler{
		metrics: metrics,
	}
}

func (h *MetricPersistHandler) HandleMessage(m *nsq.Message) error {
	ms := PersistMessage{}
	err := json.Unmarshal(m.Body, &ms)
	if err != nil {
		log.Error(3, "skipping message. %s", err)
		return nil
	}
	if ms.Instance == *instance {
		log.Debug("skipping message we generated.")
		return nil
	}

	// get metric
	if agg, ok := metrics.Get(ms.Key); ok {
		// get chunk

		chunk.Save()
	}

	return nil
}

func InitCoordinator(metrics metrics, stats met.Backend) {
	// init producers
	pCfg := nsq.NewConfig()
	pCfg.UserAgent = "metrics_tank"
	err = app.ParseOpts(pCfg, *producerOpts)
	if err != nil {
		log.Fatal(4, "failed to parse nsq producer options. %s", err)
	}
	hostPool := hostpool.NewEpsilonGreedy(strings.Split(*nsqdTCPAddrs, ","), 0, &hostpool.LinearEpsilonValueCalculator{})
	producers = make(map[string]*nsq.Producer)

	for _, addr := range strings.Split(*nsqdTCPAddrs, ",") {
		producer, err := nsq.NewProducer(addr, pCfg)
		if err != nil {
			log.Fatal(4, "failed creating producer %s", err.Error())
		}
		producers[addr] = producer
	}

	// init consumers
	cfg := nsq.NewConfig()
	cfg.UserAgent = "metrics_tank_coordinator"
	err = app.ParseOpts(cfg, *consumerOpts)
	if err != nil {
		log.Fatal(4, "failed to parse nsq consumer options. %s", err)
	}
	cfg.MaxInFlight = *maxInFlight

	consumer, err := insq.NewConsumer(*topicNotifyPersist, fmt.Sprintf("metric_tank#%06d#ephemeral", rand.Int()%999999), cfg, "metric_persist.%s", stats)
	if err != nil {
		log.Fatal(4, "Failed to create NSQ consumer. %s", err)
	}
	handler := NewMetricPersistHandler(metrics)
	consumer.AddHandler(handler)

	nsqdAdds := strings.Split(*nsqdTCPAddrs, ",")
	if len(nsqdAdds) == 1 && nsqdAdds[0] == "" {
		nsqdAdds = []string{}
	}
	err = consumer.ConnectToNSQDs(nsqdAdds)
	if err != nil {
		log.Fatal(4, "failed to connect to NSQDs. %s", err)
	}
	log.Info("persist consumer connected to nsqd")

	lookupdAdds := strings.Split(*lookupdHTTPAddrs, ",")
	if len(lookupdAdds) == 1 && lookupdAdds[0] == "" {
		lookupdAdds = []string{}
	}
	err = consumer.ConnectToNSQLookupds(lookupdAdds)
	if err != nil {
		log.Fatal(4, "failed to connect to NSQLookupds. %s", err)
	}

	return nil
}
