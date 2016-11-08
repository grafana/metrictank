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
	"github.com/raintank/met"
	"github.com/raintank/misc/app"
	"github.com/rakyll/globalconf"
)

var (
	Enabled           bool
	NsqdTCPAddrs      string
	LookupdHTTPAddrs  string
	NsqdAdds          []string
	LookupdAdds       []string
	Topic             string
	Channel           string
	MaxInFlight       int
	ProducerOpts      string
	ConsumerOpts      string
	PCfg              *nsq.Config
	CCfg              *nsq.Config
	messagesPublished met.Count
	messagesSize      met.Meter
)

func ConfigSetup() {
	fs := flag.NewFlagSet("nsq-cluster", flag.ExitOnError)
	fs.BoolVar(&Enabled, "enabled", false, "")
	fs.StringVar(&NsqdTCPAddrs, "nsqd-tcp-address", "", "nsqd TCP address (may be given multiple times as comma-separated list)")
	fs.StringVar(&LookupdHTTPAddrs, "lookupd-http-address", "", "lookupd HTTP address (may be given multiple times as comma-separated list)")
	fs.StringVar(&Topic, "topic", "metricpersist", "NSQ topic for persist messages")
	fs.StringVar(&Channel, "channel", "tank", "NSQ channel for persist messages")
	fs.StringVar(&ProducerOpts, "producer-opt", "", "option to passthrough to nsq.Producer (may be given multiple times as comma-separated list, see http://godoc.org/github.com/nsqio/go-nsq#Config)")
	fs.StringVar(&ConsumerOpts, "consumer-opt", "", "option to passthrough to nsq.Consumer (may be given multiple times as comma-separated list, http://godoc.org/github.com/nsqio/go-nsq#Config)")
	fs.IntVar(&MaxInFlight, "max-in-flight", 200, "max number of messages to allow in flight for consumer")
	globalconf.Register("nsq-cluster", fs)
}

func ConfigProcess() {
	if !Enabled {
		return
	}
	if Topic == "" {
		log.Fatal(4, "topic for nsq-cluster cannot be empty")
	}

	NsqdAdds = strings.Split(NsqdTCPAddrs, ",")
	if len(NsqdAdds) == 1 && NsqdAdds[0] == "" {
		NsqdAdds = []string{}
	}

	LookupdAdds = strings.Split(LookupdHTTPAddrs, ",")
	if len(LookupdAdds) == 1 && LookupdAdds[0] == "" {
		LookupdAdds = []string{}
	}

	// producers
	PCfg = nsq.NewConfig()
	PCfg.UserAgent = "metrictank-cluster"
	err := app.ParseOpts(PCfg, ProducerOpts)
	if err != nil {
		log.Fatal(4, "nsq-cluster: failed to parse nsq producer options. %s", err)
	}

	// consumer
	CCfg = nsq.NewConfig()
	CCfg.UserAgent = "metrictank-cluster"
	err = app.ParseOpts(CCfg, ConsumerOpts)
	if err != nil {
		log.Fatal(4, "nsq-cluster: failed to parse nsq consumer options. %s", err)
	}
	CCfg.MaxInFlight = MaxInFlight
}
