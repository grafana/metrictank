package cluster

import (
	"crypto/tls"
	"flag"
	"net"
	"net/http"
	"time"

	"github.com/raintank/worldping-api/pkg/log"
	"github.com/rakyll/globalconf"
)

var (
	ClusterName        string
	primary            bool
	peersStr           string
	mode               string
	maxPrio            int
	clusterPort        int
	clusterHost        net.IP
	clusterBindAddr    string
	httpTimeout        time.Duration
	minAvailableShards int

	client    http.Client
	transport *http.Transport
)

func ConfigSetup() {
	clusterCfg := flag.NewFlagSet("cluster", flag.ExitOnError)
	clusterCfg.StringVar(&ClusterName, "name", "metrictank", "Unique name of the cluster.")
	clusterCfg.BoolVar(&primary, "primary-node", false, "the primary node writes data to cassandra. There should only be 1 primary node per shardGroup.")
	clusterCfg.StringVar(&clusterBindAddr, "bind-addr", "0.0.0.0:7946", "TCP Address to listen on for cluster communication")
	clusterCfg.StringVar(&peersStr, "peers", "", "TCP addresses of other nodes, comma separated. use this if you shard your data and want to query other instances")
	clusterCfg.StringVar(&mode, "mode", "single", "Operating mode of cluster. (single|multi)")
	clusterCfg.DurationVar(&httpTimeout, "http-timeout", time.Second*60, "How long to wait before aborting http requests to cluster peers and returning a http 503 service unavailable")
	clusterCfg.IntVar(&maxPrio, "max-priority", 10, "maximum priority before a node should be considered not-ready.")
	clusterCfg.IntVar(&minAvailableShards, "min-available-shards", 0, "minimum number of shards that must be available for a query to be handled.")
	globalconf.Register("cluster", clusterCfg)
}

func ConfigProcess() {
	if !validMode(mode) {
		log.Fatal(4, "CLU Config: invalid cluster operating mode")
	}

	addr, err := net.ResolveTCPAddr("tcp", clusterBindAddr)
	if err != nil {
		log.Fatal(4, "CLU Config: bind-addr is not a valid TCP address: %s", err.Error())
	}

	if httpTimeout == 0 {
		log.Fatal(4, "CLU Config: http-timeout must be a non-zero duration string like 60s")
	}

	clusterHost = addr.IP
	clusterPort = addr.Port

	Mode = ModeType(mode)
	transport = &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		Proxy:           http.ProxyFromEnvironment,
		Dial: (&net.Dialer{
			Timeout:   time.Second * 5,
			KeepAlive: 30 * time.Second,
		}).Dial,
		TLSHandshakeTimeout: time.Second,
	}
	client = http.Client{
		Transport: transport,
		Timeout:   httpTimeout,
	}
}
