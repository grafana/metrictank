package api

import (
	"flag"
	"net"
	"net/http/httputil"
	"net/url"
	"time"

	"github.com/raintank/dur"
	"github.com/rakyll/globalconf"
	log "github.com/sirupsen/logrus"
)

var (
	maxPointsPerReqSoft int
	maxPointsPerReqHard int
	maxSeriesPerReq     int
	logMinDurStr        string
	logMinDur           uint32

	Addr             string
	UseSSL           bool
	useGzip          bool
	certFile         string
	keyFile          string
	multiTenant      bool
	fallbackGraphite string
	timeZoneStr      string

	getTargetsConcurrency int
	tagdbDefaultLimit     uint
	speculationThreshold  float64

	graphiteProxy *httputil.ReverseProxy
	timeZone      *time.Location
)

func ConfigSetup() {
	apiCfg := flag.NewFlagSet("http", flag.ExitOnError)
	apiCfg.IntVar(&maxPointsPerReqSoft, "max-points-per-req-soft", 1000000, "lower resolution rollups will be used to try and keep requests below this number of datapoints. (0 disables limit)")
	apiCfg.IntVar(&maxPointsPerReqHard, "max-points-per-req-hard", 20000000, "limit of number of datapoints a request can return. Requests that exceed this limit will be rejected. (0 disables limit)")
	apiCfg.IntVar(&maxSeriesPerReq, "max-series-per-req", 250000, "limit of number of series a request can operate on. Requests that exceed this limit will be rejected. (0 disables limit)")
	apiCfg.StringVar(&logMinDurStr, "log-min-dur", "5min", "only log incoming requests if their timerange is at least this duration. Use 0 to disable")

	apiCfg.StringVar(&Addr, "listen", ":6060", "http listener address.")
	apiCfg.BoolVar(&UseSSL, "ssl", false, "use HTTPS")
	apiCfg.BoolVar(&useGzip, "gzip", true, "use GZIP compression of all responses")
	apiCfg.StringVar(&certFile, "cert-file", "", "SSL certificate file")
	apiCfg.StringVar(&keyFile, "key-file", "", "SSL key file")
	apiCfg.BoolVar(&multiTenant, "multi-tenant", true, "require x-org-id authentication to auth as a specific org. otherwise orgId 1 is assumed")
	apiCfg.StringVar(&fallbackGraphite, "fallback-graphite-addr", "http://localhost:8080", "in case our /render endpoint does not support the requested processing, proxy the request to this graphite")
	apiCfg.StringVar(&timeZoneStr, "time-zone", "local", "timezone for interpreting from/until values when needed, specified using [zoneinfo name](https://en.wikipedia.org/wiki/Tz_database#Names_of_time_zones) e.g. 'America/New_York', 'UTC' or 'local' to use local server timezone")
	apiCfg.IntVar(&getTargetsConcurrency, "get-targets-concurrency", 20, "maximum number of concurrent threads for fetching data on the local node. Each thread handles a single series.")
	apiCfg.UintVar(&tagdbDefaultLimit, "tagdb-default-limit", 100, "default limit for tagdb query results, can be overridden with query parameter \"limit\"")
	apiCfg.Float64Var(&speculationThreshold, "speculation-threshold", 1, "ratio of peer responses after which speculation is used. Set to 1 to disable.")
	globalconf.Register("http", apiCfg)
}

func ConfigProcess() {
	logMinDur = dur.MustParseDuration("log-min-dur", logMinDurStr)

	//validate the addr
	_, err := net.ResolveTCPAddr("tcp", Addr)
	if err != nil {
		log.Fatal("API listen address is not a valid TCP address.")
	}

	u, err := url.Parse(fallbackGraphite)
	if err != nil {
		log.Fatalf("API Cannot parse fallback-graphite-addr: %s", err.Error())
	}
	graphiteProxy = NewGraphiteProxy(u)

	if timeZoneStr == "local" {
		timeZone = time.Local
	} else {
		timeZone, err = time.LoadLocation(timeZoneStr)
		if err != nil {
			log.Fatalf("API Cannot load timezone %q: %s", timeZoneStr, err.Error())
		}
	}
}
