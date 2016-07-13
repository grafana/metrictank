package nsq

import (
	"strings"
	"time"

	"github.com/nsqio/go-nsq"
	"github.com/raintank/met"
	"github.com/raintank/metrictank/defcache"
	"github.com/raintank/metrictank/mdata"
	"github.com/raintank/metrictank/usage"
	"github.com/raintank/misc/app"
	"github.com/raintank/misc/instrumented_nsq"
	"github.com/raintank/worldping-api/pkg/log"
)

type NSQ struct {
	nsqdAdds    []string
	lookupdAdds []string
	consumer    *insq.Consumer
	concurrency int
	stats       met.Backend
	StopChan    chan int
}

func New(consumerOpts, nsqdTCPAddrs, lookupdHTTPAddrs, topic, channel string, maxInFlight int, concurrency int, stats met.Backend) *NSQ {
	cfg := nsq.NewConfig()
	cfg.UserAgent = "metrics_tank"
	err := app.ParseOpts(cfg, consumerOpts)
	if err != nil {
		log.Fatal(4, "failed to parse nsq consumer options. %s", err)
	}
	cfg.MaxInFlight = maxInFlight
	n := NSQ{
		concurrency: concurrency,
		stats:       stats,
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

func (n *NSQ) Start(metrics mdata.Metrics, defCache *defcache.DefCache, usg *usage.Usage) {
	for i := 0; i < n.concurrency; i++ {
		handler := NewHandler(metrics, defCache, usg, n.stats)
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
