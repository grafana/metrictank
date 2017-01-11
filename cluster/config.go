package cluster

import (
	"flag"
	"net"

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
)

func ConfigSetup() {
	clusterCfg := flag.NewFlagSet("cluster", flag.ExitOnError)
	clusterCfg.BoolVar(&primary, "primary-node", false, "the primary node writes data to cassandra. There should only be 1 primary node per shardGroup.")
	clusterCfg.StringVar(&clusterBindAddr, "bind-addr", "0.0.0.0:7946", "TCP Address to listen on for cluster communication")
	clusterCfg.StringVar(&peersStr, "peers", "", "TCP addresses of other nodes, comma separated. use this if you shard your data and want to query other instances")
	clusterCfg.StringVar(&mode, "mode", "single", "Operating mode of cluster. (single|multi)")
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

	clusterHost = addr.IP
	clusterPort = addr.Port

	Mode = ModeType(mode)
}
