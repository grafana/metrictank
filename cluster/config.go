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
	primary         bool
	peersStr        string
	mode            string
	clusterPort     int
	clusterHost     net.IP
	clusterBindAddr string
	httpTimeout     time.Duration

	client http.Client
)

func ConfigSetup() {
	clusterCfg := flag.NewFlagSet("cluster", flag.ExitOnError)
	clusterCfg.BoolVar(&primary, "primary-node", false, "the primary node writes data to cassandra. There should only be 1 primary node per shardGroup.")
	clusterCfg.StringVar(&clusterBindAddr, "bind-addr", "0.0.0.0:7946", "TCP Address to listen on for cluster communication")
	clusterCfg.StringVar(&peersStr, "peers", "", "TCP addresses of other nodes, comma separated. use this if you shard your data and want to query other instances")
	clusterCfg.StringVar(&mode, "mode", "single", "Operating mode of cluster. (single|multi)")
	clusterCfg.DurationVar(&httpTimeout, "http-timeout", time.Second*60, "How long to wait before aborting http requests to cluster peers and returning a http 503 service unavailable")
	globalconf.Register("cluster", clusterCfg)
}

func ConfigProcess() {
	if !validMode(mode) {
		log.Fatal(4, "CLU Config: invalid cluster operating mode")
	}

	addr, err := net.ResolveTCPAddr("tcp", clusterBindAddr)
	if err != nil {
		log.Fatal(4, "CLU Config: cluster-bind-addres is not a valid TCP address: %s", err.Error())
	}

	if httpTimeout == 0 {
		log.Fatal(4, "CLU Config: http-timeout must be a non-zero duration string like 60s")
	}

	clusterHost = addr.IP
	clusterPort = addr.Port

	Mode = ModeType(mode)

	client = http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
			Proxy:           http.ProxyFromEnvironment,
			Dial: (&net.Dialer{
				Timeout:   time.Second * 5,
				KeepAlive: 30 * time.Second,
			}).Dial,
			TLSHandshakeTimeout: time.Second,
		},
		Timeout: httpTimeout,
	}
}
