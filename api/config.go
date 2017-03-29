package api

import (
	"flag"
	"net"

	"github.com/raintank/dur"
	"github.com/raintank/worldping-api/pkg/log"
	"github.com/rakyll/globalconf"
)

var (
	maxPointsPerReqSoft int
	maxPointsPerReqHard int
	logMinDurStr        string
	logMinDur           uint32

	Addr     string
	UseSSL   bool
	useGzip  bool
	certFile string
	keyFile  string
)

func ConfigSetup() {
	apiCfg := flag.NewFlagSet("http", flag.ExitOnError)
	apiCfg.IntVar(&maxPointsPerReqSoft, "max-points-per-req-soft", 1000000, "lower resolution rollups will be used to try and keep requests below this number of datapoints. (0 disables limit)")
	apiCfg.IntVar(&maxPointsPerReqHard, "max-points-per-req-hard", 20000000, "limit of number of datapoints a request can return. Requests that exceed this limit will be rejected. (0 disables limit)")
	apiCfg.StringVar(&logMinDurStr, "log-min-dur", "5min", "only log incoming requests if their timerange is at least this duration. Use 0 to disable")

	apiCfg.StringVar(&Addr, "listen", ":6060", "http listener address.")
	apiCfg.BoolVar(&UseSSL, "ssl", false, "use HTTPS")
	apiCfg.BoolVar(&useGzip, "gzip", true, "use GZIP compression of all responses")
	apiCfg.StringVar(&certFile, "cert-file", "", "SSL certificate file")
	apiCfg.StringVar(&keyFile, "key-file", "", "SSL key file")
	globalconf.Register("http", apiCfg)
}

func ConfigProcess() {
	logMinDur = dur.MustParseUsec("log-min-dur", logMinDurStr)

	//validate the addr
	_, err := net.ResolveTCPAddr("tcp", Addr)
	if err != nil {
		log.Fatal(4, "API listen address is not a valid TCP address.")
	}
}
