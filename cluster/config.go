package cluster

import (
	"flag"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/raintank/worldping-api/pkg/log"
	"github.com/rakyll/globalconf"
)

var (
	primary          bool
	peersStr         string
	probeIntervalStr string
	probeInterval    time.Duration
	mode             string
)

func ConfigSetup() {
	clusterCfg := flag.NewFlagSet("cluster", flag.ExitOnError)
	clusterCfg.BoolVar(&primary, "primary-node", false, "the primary node writes data to cassandra. There should only be 1 primary node per shardGroup.")
	clusterCfg.StringVar(&peersStr, "peers", "", "http/s addresses of other nodes, comma separated. use this if you shard your data and want to query other instances")
	clusterCfg.StringVar(&probeIntervalStr, "probe-interval", "2s", "Interval to probe peer nodes")
	clusterCfg.StringVar(&mode, "mode", "single", "Operating mode of cluster. (single|multi)")
	globalconf.Register("cluster", clusterCfg)
}

func ConfigProcess() {
	var err error
	probeInterval, err = time.ParseDuration(probeIntervalStr)
	if err != nil {
		log.Fatal(4, "probe-interval could not be parsed. %s", err)
	}

	if !validMode(mode) {
		log.Fatal(4, "invalid cluster operating mode")
	}

	Mode = ModeType(mode)

	if peersStr != "" {
		for _, peer := range strings.Split(peersStr, ",") {
			// if peers are listed as "host:port" then assume they are using http
			if !strings.HasPrefix(peer, "http://") && !strings.HasPrefix(peer, "https://") {
				peer = fmt.Sprintf("http://%s", peer)
			}
			peer = strings.TrimSuffix(peer, "/")
			addr, err := url.Parse(peer)
			if err != nil {
				log.Fatal(4, "Unable to parse Peer address %s: %s", peer, err)
			}
			AddPeer(addr)
		}
	}
	ThisNode.SetPrimary(primary)
}
