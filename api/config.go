package api

import (
	"flag"

	"github.com/raintank/dur"
	"github.com/rakyll/globalconf"
)

var (
	maxPointsPerReq int
	maxDaysPerReq   int
	logMinDurStr    string
	logMinDur       uint32
)

func ConfigSetup() {
	apiCfg := flag.NewFlagSet("http", flag.ExitOnError)
	apiCfg.IntVar(&maxDaysPerReq, "max-days-per-req", 365000, "max amount of days range for one request. the default allows 500 series of 2 year each. (0 disables limit")
	apiCfg.IntVar(&maxPointsPerReq, "max-points-per-req", 1000000, "max points could be requested in one request. 1M allows 500 series at a MaxDataPoints of 2000. (0 disables limit)")
	apiCfg.StringVar(&logMinDurStr, "log-min-dur", "5min", "only log incoming requests if their timerange is at least this duration. Use 0 to disable")
	globalconf.Register("http", apiCfg)
}

func ConfigProcess() {
	logMinDur = dur.MustParseUsec("log-min-dur", logMinDurStr)
}
