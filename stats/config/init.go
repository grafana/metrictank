package config

import (
	"flag"
	"strings"

	"github.com/raintank/metrictank/stats"
	"github.com/raintank/worldping-api/pkg/log"
	"github.com/rakyll/globalconf"
)

var enabled bool
var prefix string
var addr string
var interval int
var bufferSize int

func ConfigSetup() {
	inStats := flag.NewFlagSet("stats", flag.ExitOnError)
	inStats.BoolVar(&enabled, "enabled", true, "enable sending graphite messages for instrumentation")
	inStats.StringVar(&prefix, "prefix", "metrictank.stats.default.$instance", "stats prefix (will add trailing dot automatically if needed)")
	inStats.StringVar(&addr, "addr", "localhost:2003", "graphite address")
	inStats.IntVar(&interval, "interval", 1, "interval at which to send statistics")
	inStats.IntVar(&bufferSize, "buffer-size", 20000, "how many messages (holding all measurements from one interval. rule of thumb: a message is ~25kB) to buffer up in case graphite endpoint is unavailable. With the default of 20k you will use max about 500MB and bridge 5 hours of downtime when needed")
	globalconf.Register("stats", inStats)
}

func ConfigProcess(instance string) {
	if !enabled {
		return
	}
	// TODO validate tcp addr
	prefix = strings.Replace(prefix, "$instance", instance, -1)
}

func Start() {
	if enabled {
		stats.NewMemoryReporter()
		stats.NewGraphite(prefix, addr, interval, bufferSize)
	} else {
		stats.NewDevnull()
		log.Warn("running metrictank without instrumentation.")
	}
}
