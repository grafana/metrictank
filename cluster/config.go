package cluster

import (
	"crypto/tls"
	"flag"
	"net"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/grafana/globalconf"
	log "github.com/sirupsen/logrus"
)

var (
	ClusterName        string
	primary            bool
	peersStr           string
	mode               string
	maxPrio            int
	httpTimeout        time.Duration
	minAvailableShards int
	gcPercent          int
	gcPercentNotReady  int
	gcRunInterval      int
	gcRunJitter        int
	GossipSettlePeriod time.Duration // if gossip not enabled, will be 0 regardless of config

	gossipSettlePeriodStr string

	swimUseConfig               = "default-lan"
	swimAdvertiseAddrStr        string
	swimAdvertiseAddr           *net.TCPAddr
	swimBindAddrStr             string
	swimBindAddr                *net.TCPAddr
	swimTCPTimeout              time.Duration
	swimIndirectChecks          int
	swimRetransmitMult          int
	swimSuspicionMult           int
	swimSuspicionMaxTimeoutMult int
	swimPushPullInterval        time.Duration
	swimProbeInterval           time.Duration
	swimProbeTimeout            time.Duration
	swimDisableTcpPings         bool
	swimAwarenessMaxMultiplier  int
	swimGossipInterval          time.Duration
	swimGossipNodes             int
	swimGossipToTheDeadTime     time.Duration
	swimEnableCompression       bool
	swimDNSConfigPath           string

	client    http.Client
	transport *http.Transport
)

func ConfigSetup() {

	// behavior here is same as standard runtime:
	// unparseable or not set -> 100
	// "off" -> -1
	gcPercent = 100
	gogc := os.Getenv("GOGC")
	if gogc != "" {
		if gogc == "off" {
			gcPercent = -1
		}
		val, err := strconv.Atoi(gogc)
		if err == nil {
			gcPercent = val
		}
	}

	clusterCfg := flag.NewFlagSet("cluster", flag.ExitOnError)
	clusterCfg.StringVar(&ClusterName, "name", "metrictank", "Unique name of the cluster.")
	clusterCfg.BoolVar(&primary, "primary-node", false, "the primary node writes data to cassandra. There should only be 1 primary node per shardGroup.")
	clusterCfg.StringVar(&peersStr, "peers", "", "TCP addresses of other nodes, comma separated. use this if you shard your data and want to query other instances")
	clusterCfg.StringVar(&mode, "mode", "dev", "Operating mode of this instance within the cluster. (dev|shard|query)")
	clusterCfg.DurationVar(&httpTimeout, "http-timeout", time.Second*60, "How long to wait before aborting http requests to cluster peers and returning a http 503 service unavailable")
	clusterCfg.IntVar(&maxPrio, "max-priority", 10, "maximum priority before a node should be considered not-ready.")
	clusterCfg.IntVar(&minAvailableShards, "min-available-shards", 0, "minimum number of shards that must be available for a query to be handled.")
	clusterCfg.IntVar(&gcPercentNotReady, "gc-percent-not-ready", gcPercent, "GOGC value to use when node is not ready.  Defaults to GOGC")
	clusterCfg.IntVar(&gcRunInterval, "gc-run-interval", -1, "Interval to force a GC run. Defaults to -1 (off)")
	clusterCfg.IntVar(&gcRunJitter, "gc-run-jitter", 0, "Jitter for GC run as an offset of interval. Useful for staggering replica peers GC. Defaults to 0 (run on multiples of interval)")
	clusterCfg.StringVar(&gossipSettlePeriodStr, "gossip-settle-period", "10s", "duration until when the cluster topology can be considered up-to-date and this node to be ready to serve requests (when gossip enabled).")
	globalconf.Register("cluster", clusterCfg, flag.ExitOnError)

	swimCfg := flag.NewFlagSet("swim", flag.ExitOnError)
	swimCfg.StringVar(&swimUseConfig, "use-config", "manual", "config setting to use. If set to anything but manual, will override all other swim settings. Use manual|default-lan|default-local|default-wan. see https://godoc.org/github.com/hashicorp/memberlist#Config . Note all our swim settings correspond to default-lan")
	swimCfg.StringVar(&swimBindAddrStr, "bind-addr", "0.0.0.0:7946", "binding TCP Address for UDP and TCP gossip")
	swimCfg.StringVar(&swimAdvertiseAddrStr, "advertise-addr", "", "advertised TCP address for UDP and TCP gossip (full ip/dns:port combo, or empty to use bind-addr)")
	swimCfg.DurationVar(&swimTCPTimeout, "tcp-timeout", 10*time.Second, "timeout for establishing a stream connection with peers for a full state sync, and for stream reads and writes")
	swimCfg.IntVar(&swimIndirectChecks, "indirect-checks", 3, "number of nodes that will be asked to perform an indirect probe of a node in the case a direct probe fails")
	swimCfg.IntVar(&swimRetransmitMult, "retransmit-mult", 4, "multiplier for number of retransmissions for gossip messages. Retransmits = RetransmitMult * log(N+1)")
	swimCfg.IntVar(&swimSuspicionMult, "suspicion-multi", 4, "multiplier for determining when inaccessible/suspect node is delared dead. SuspicionTimeout = SuspicionMult * log(N+1) * ProbeInterval")
	swimCfg.IntVar(&swimSuspicionMaxTimeoutMult, "suspicion-max-timeout-mult", 6, "multiplier for upper bound on detection time.  SuspicionMaxTimeout = SuspicionMaxTimeoutMult * SuspicionTimeout")
	swimCfg.DurationVar(&swimPushPullInterval, "push-pull-interval", 30*time.Second, "interval between complete state syncs. 0 will disable state push/pull syncs")
	swimCfg.DurationVar(&swimProbeInterval, "probe-interval", 1*time.Second, "interval between random node probes")
	swimCfg.DurationVar(&swimProbeTimeout, "probe-timeout", 500*time.Millisecond, "timeout to wait for an ack from a probed node before assuming it is unhealthy. This should be set to 99-percentile of network RTT")
	swimCfg.BoolVar(&swimDisableTcpPings, "disable-tcp-pings", false, "turn off the fallback TCP pings that are attempted if the direct UDP ping fails")
	swimCfg.IntVar(&swimAwarenessMaxMultiplier, "awareness-max-multiplier", 8, "will increase the probe interval if the node becomes aware that it might be degraded and not meeting the soft real time requirements to reliably probe other nodes.")
	swimCfg.IntVar(&swimGossipNodes, "gossip-nodes", 3, "number of random nodes to send gossip messages to per GossipInterval")
	swimCfg.DurationVar(&swimGossipInterval, "gossip-interval", 200*time.Millisecond, "interval between sending messages that need to be gossiped that haven't been able to piggyback on probing messages. 0 disables non-piggyback gossip")
	swimCfg.DurationVar(&swimGossipToTheDeadTime, "gossip-to-the-dead-time", 30*time.Second, "interval after which a node has died that we will still try to gossip to it. This gives it a chance to refute")
	swimCfg.BoolVar(&swimEnableCompression, "enable-compression", true, "message compression")
	swimCfg.StringVar(&swimDNSConfigPath, "dns-config-path", "/etc/resolv.conf", "system's DNS config file. Override allows for easier testing")
	globalconf.Register("swim", swimCfg, flag.ExitOnError)
}

func ConfigProcess() {
	// check settings in cluster section
	var err error
	Mode, err = NodeModeFromString(mode)
	if err != nil {
		log.Fatalf("CLU Config: %s", err.Error())
	}

	if httpTimeout == 0 {
		log.Fatal("CLU Config: http-timeout must be a non-zero duration string like 60s")
	}

	transport = &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		Proxy:           http.ProxyFromEnvironment,
		Dial: (&net.Dialer{
			Timeout:   time.Second * 5,
			KeepAlive: 30 * time.Second,
		}).Dial,
		TLSHandshakeTimeout: time.Second,
		MaxIdleConns:        1000,
		MaxIdleConnsPerHost: 100,
	}
	client = http.Client{
		Transport: transport,
		Timeout:   httpTimeout,
	}

	// all further stuff is only relevant in shard/query mode
	if Mode == ModeDev {
		return
	}

	GossipSettlePeriod, err = time.ParseDuration(gossipSettlePeriodStr)
	if err != nil {
		log.Fatalf("CLU Config: invalid gossip-settle-period: %s", err.Error())
	}

	// check settings in swim section
	if swimUseConfig != "manual" && swimUseConfig != "default-lan" && swimUseConfig != "default-local" && swimUseConfig != "default-wan" {
		log.Fatal("CLU Config: invalid swim-use-config setting")
	}

	if swimUseConfig == "manual" {
		var err error
		swimBindAddr, err = net.ResolveTCPAddr("tcp", swimBindAddrStr)
		if err != nil {
			log.Fatalf("CLU Config: swim-bind-addr is not a valid TCP address: %s", err.Error())
		}

		if swimAdvertiseAddrStr != "" {
			swimAdvertiseAddr, err = net.ResolveTCPAddr("tcp", swimAdvertiseAddrStr)
			if err != nil {
				log.Fatalf("CLU Config: swim-advertise-addr is not a valid TCP address: %s", err.Error())
			}
		}
	}
}
