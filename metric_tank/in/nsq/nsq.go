package nsq

import (
	"strings"
	"time"

	"github.com/grafana/grafana/pkg/log"
	"github.com/nsqio/go-nsq"
	"github.com/raintank/met"
	"github.com/raintank/raintank-metric/app"
	"github.com/raintank/raintank-metric/instrumented_nsq"
	"github.com/raintank/raintank-metric/metric_tank/defcache"
	"github.com/raintank/raintank-metric/metric_tank/mdata"
	"github.com/raintank/raintank-metric/metric_tank/usage"
)

var (
	metricsPerMessage met.Meter
	metricsReceived   met.Count
	msgsAge           met.Meter // in ms
)

type NSQ struct {
	nsqdAdds    []string
	lookupdAdds []string
	consumer    *insq.Consumer
	concurrency int
	metrics     mdata.Metrics
	defCache    *defcache.DefCache
	StopChan    chan int
}

func New(consumerOpts, nsqdTCPAddrs, lookupdHTTPAddrs, topic, channel string, maxInFlight int, concurrency int, metrics mdata.Metrics, defCache *defcache.DefCache, stats met.Backend) *NSQ {
	metricsPerMessage = stats.NewMeter("metrics_per_message", 0)
	metricsReceived = stats.NewCount("metrics_received")
	msgsAge = stats.NewMeter("message_age", 0)

	cfg := nsq.NewConfig()
	cfg.UserAgent = "metrics_tank"
	err := app.ParseOpts(cfg, consumerOpts)
	if err != nil {
		log.Fatal(4, "failed to parse nsq consumer options. %s", err)
	}
	cfg.MaxInFlight = maxInFlight
	n := NSQ{
		concurrency: concurrency,
		metrics:     metrics,
		defCache:    defCache,
	}

	consumer, err := insq.NewConsumer(topic, channel, cfg, "%s", stats)
	if err != nil {
		log.Fatal(4, "Failed to create NSQ consumer. %s", err)
	}
	n.consumer = consumer
	n.StopChan = n.consumer.StopChan

	nsqdAdds := strings.Split(nsqdTCPAddrs, ",")
	if len(nsqdAdds) == 1 && nsqdAdds[0] == "" {
		nsqdAdds = []string{}
	}

	lookupdAdds := strings.Split(lookupdHTTPAddrs, ",")
	if len(lookupdAdds) == 1 && lookupdAdds[0] == "" {
		lookupdAdds = []string{}
	}
	n.nsqdAdds = nsqdAdds
	n.lookupdAdds = lookupdAdds
	return &n

}

func (n *NSQ) Start(usg *usage.Usage) {
	for i := 0; i < n.concurrency; i++ {
		handler := NewHandler(n.metrics, n.defCache, usg)
		n.consumer.AddHandler(handler)
	}
	time.Sleep(100 * time.Millisecond)
	err := n.consumer.ConnectToNSQDs(n.nsqdAdds)
	if err != nil {
		log.Fatal(4, "failed to connect to NSQDs. %s", err)
	}
	log.Info("consumer connected to nsqd")

	err = n.consumer.ConnectToNSQLookupds(n.lookupdAdds)
	if err != nil {
		log.Fatal(4, "failed to connect to NSQLookupds. %s", err)
	}
	log.Info("consumer connected to nsqlookupd")

}

// Stop will initiate a graceful stop of the Consumer (permanent)
//
// NOTE: receive on StopChan to block until this process completes
func (n *NSQ) Stop() {
	n.consumer.Stop()
}
