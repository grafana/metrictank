package api

import (
	"flag"
	"net"

	"github.com/raintank/dur"
	"github.com/raintank/worldping-api/pkg/log"
	"github.com/rakyll/globalconf"
)

var (
	maxPointsPerReq int
	maxDaysPerReq   int
	logMinDurStr    string
	logMinDur       uint32

	Addr     string
	UseSSL   bool
	certFile string
	keyFile  string
)

func ConfigSetup() {
	apiCfg := flag.NewFlagSet("http", flag.ExitOnError)
	apiCfg.IntVar(&maxDaysPerReq, "max-days-per-req", 365000, "max number of days range for one request. the default allows 500 series of 2 year each. (0 disables limit")
	apiCfg.IntVar(&maxPointsPerReq, "max-points-per-req", 1000000, "max points could be requested in one request. 1M allows 500 series at a MaxDataPoints of 2000. (0 disables limit)")
	apiCfg.StringVar(&logMinDurStr, "log-min-dur", "5min", "only log incoming requests if their timerange is at least this duration. Use 0 to disable")

	apiCfg.StringVar(&Addr, "listen", ":6060", "http listener address.")
	apiCfg.BoolVar(&UseSSL, "ssl", false, "use HTTPS")
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
