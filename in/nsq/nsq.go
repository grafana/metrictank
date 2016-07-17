package nsq

import (
	"flag"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/bitly/go-hostpool"
	"github.com/nsqio/go-nsq"
	"github.com/raintank/met"
	"github.com/raintank/metrictank/defcache"
	"github.com/raintank/metrictank/mdata"
	"github.com/raintank/metrictank/usage"
	"github.com/raintank/misc/app"
	"github.com/raintank/misc/instrumented_nsq"
	"github.com/raintank/worldping-api/pkg/log"
	"github.com/rakyll/globalconf"
)

var (
	hostPool         hostpool.HostPool
	producers        map[string]*nsq.Producer
	Enabled          bool
	nsqdTCPAddrs     string
	lookupdHTTPAddrs string
	nsqdAdds         []string
	lookupdAdds      []string
	topic            string
	channel          string
	maxInFlight      int
	producerOpts     string
	consumerOpts     string
	concurrency      int
	cfg              *nsq.Config
)

type NSQ struct {
	consumer *insq.Consumer
	stats    met.Backend
	StopChan chan int
}

func ConfigSetup() {
	fs := flag.NewFlagSet("nsq-in", flag.ExitOnError)
	fs.BoolVar(&Enabled, "enabled", false, "")
	fs.StringVar(&nsqdTCPAddrs, "nsqd-tcp-address", "", "nsqd TCP address (may be given multiple times as comma-separated list)")
	fs.StringVar(&lookupdHTTPAddrs, "lookupd-http-address", "", "lookupd HTTP address (may be given multiple times as comma-separated list)")
	fs.StringVar(&topic, "topic", "metrics", "NSQ topic for metrics")
	fs.StringVar(&channel, "channel", "tank", "NSQ channel for metrics. leave empty to generate random ephemeral one")
	fs.StringVar(&producerOpts, "producer-opt", "", "option to passthrough to nsq.Producer (may be given multiple times as comma-separated list, see http://godoc.org/github.com/nsqio/go-nsq#Config)")
	fs.StringVar(&consumerOpts, "consumer-opt", "", "option to passthrough to nsq.Consumer (may be given multiple times as comma-separated list, http://godoc.org/github.com/nsqio/go-nsq#Config)")
	fs.IntVar(&maxInFlight, "max-in-flight", 200, "max number of messages to allow in flight")
	fs.IntVar(&concurrency, "concurrency", 10, "number of workers parsing messages from NSQ")
	globalconf.Register("nsq-in", fs)
}

func ConfigProcess() {
	if channel == "" {
		rand.Seed(time.Now().UnixNano())
		channel = fmt.Sprintf("metrictank%06d#ephemeral", rand.Int()%999999)
	}

	if topic == "" {
		log.Fatal(4, "topic is required")
	}

	if nsqdTCPAddrs == "" && lookupdHTTPAddrs == "" {
		log.Fatal(4, "nsqd-tcp-address or lookupd-http-address required")
	}

	if nsqdTCPAddrs != "" && lookupdHTTPAddrs != "" {
		log.Fatal(4, "use nsqd-tcp-address or lookupd-http-address not both")
	}

	cfg = nsq.NewConfig()
	cfg.UserAgent = "metrictank"
	err := app.ParseOpts(cfg, consumerOpts)
	if err != nil {
		log.Fatal(4, "failed to parse nsq consumer options. %s", err)
	}

	cfg.MaxInFlight = maxInFlight

	nsqdAdds = strings.Split(nsqdTCPAddrs, ",")
	if len(nsqdAdds) == 1 && nsqdAdds[0] == "" {
		nsqdAdds = []string{}
	}

	lookupdAdds = strings.Split(lookupdHTTPAddrs, ",")
	if len(lookupdAdds) == 1 && lookupdAdds[0] == "" {
		lookupdAdds = []string{}
	}
}

func New(stats met.Backend) *NSQ {
	consumer, err := insq.NewConsumer(topic, channel, cfg, "%s", stats)
	if err != nil {
		log.Fatal(4, "Failed to create NSQ consumer. %s", err)
	}
	return &NSQ{
		stats:    stats,
		consumer: consumer,
		StopChan: consumer.StopChan,
	}
}

func (n *NSQ) Start(metrics mdata.Metrics, defCache *defcache.DefCache, usg *usage.Usage) {
	for i := 0; i < concurrency; i++ {
		handler := NewHandler(metrics, defCache, usg, n.stats)
		n.consumer.AddHandler(handler)
	}
	time.Sleep(100 * time.Millisecond)
	err := n.consumer.ConnectToNSQDs(nsqdAdds)
	if err != nil {
		log.Fatal(4, "failed to connect to NSQDs. %s", err)
	}
	log.Info("consumer connected to nsqd")

	err = n.consumer.ConnectToNSQLookupds(lookupdAdds)
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
