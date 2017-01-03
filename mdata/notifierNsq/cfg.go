// this package provides a separate namespace for clustered nsq.
// well.. the problem is we can't refer to anything in mdata here, such as mdata.Metrics
// which the clustered nsq plugin needs.  But we want a separate scope so we can cleanly name
// and refer to the config variables (clNSQ.Enabled etc), so we currently have this hybrid approach
// with config handling in this package, but the rest of clNSQ is in the mdata package

package notifierNsq

import (
	"flag"
	"log"
	"strings"

	"github.com/nsqio/go-nsq"
	"github.com/raintank/metrictank/stats"
	"github.com/raintank/misc/app"
	"github.com/rakyll/globalconf"
)

var (
	Enabled           bool
	nsqdTCPAddrs      string
	lookupdHTTPAddrs  string
	nsqdAdds          []string
	lookupdAdds       []string
	topic             string
	channel           string
	maxInFlight       int
	producerOpts      string
	consumerOpts      string
	pCfg              *nsq.Config
	cCfg              *nsq.Config
	messagesPublished *stats.Counter32
	messagesSize      *stats.Meter32
)

func ConfigSetup() {
	fs := flag.NewFlagSet("nsq-cluster", flag.ExitOnError)
	fs.BoolVar(&Enabled, "enabled", false, "")
	fs.StringVar(&nsqdTCPAddrs, "nsqd-tcp-address", "", "nsqd TCP address (may be given multiple times as comma-separated list)")
	fs.StringVar(&lookupdHTTPAddrs, "lookupd-http-address", "", "lookupd HTTP address (may be given multiple times as comma-separated list)")
	fs.StringVar(&topic, "topic", "metricpersist", "NSQ topic for persist messages")
	fs.StringVar(&channel, "channel", "tank", "NSQ channel for persist messages")
	fs.StringVar(&producerOpts, "producer-opt", "", "option to passthrough to nsq.Producer (may be given multiple times as comma-separated list, see http://godoc.org/github.com/nsqio/go-nsq#Config)")
	fs.StringVar(&consumerOpts, "consumer-opt", "", "option to passthrough to nsq.Consumer (may be given multiple times as comma-separated list, http://godoc.org/github.com/nsqio/go-nsq#Config)")
	fs.IntVar(&maxInFlight, "max-in-flight", 200, "max number of messages to allow in flight for consumer")
	globalconf.Register("nsq-cluster", fs)
}

func ConfigProcess() {
	if !Enabled {
		return
	}
	if topic == "" {
		log.Fatal(4, "topic for nsq-cluster cannot be empty")
	}

	nsqdAdds = strings.Split(nsqdTCPAddrs, ",")
	if len(nsqdAdds) == 1 && nsqdAdds[0] == "" {
		nsqdAdds = []string{}
	}

	lookupdAdds = strings.Split(lookupdHTTPAddrs, ",")
	if len(lookupdAdds) == 1 && lookupdAdds[0] == "" {
		lookupdAdds = []string{}
	}

	// producers
	pCfg = nsq.NewConfig()
	pCfg.UserAgent = "metrictank-cluster"
	err := app.ParseOpts(pCfg, producerOpts)
	if err != nil {
		log.Fatal(4, "nsq-cluster: failed to parse nsq producer options. %s", err)
	}

	// consumer
	cCfg = nsq.NewConfig()
	cCfg.UserAgent = "metrictank-cluster"
	err = app.ParseOpts(cCfg, consumerOpts)
	if err != nil {
		log.Fatal(4, "nsq-cluster: failed to parse nsq consumer options. %s", err)
	}
	cCfg.MaxInFlight = maxInFlight
}
