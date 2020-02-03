package cmd

import (
	"github.com/raintank/worldping-api/pkg/log"
	"os"
	"strings"

	"github.com/raintank/met/helper"
)

func initStats(enabled bool, service string) {
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatal(4, "failed to lookup hostname. %s", err)
	}
	service = "fakemetrics." + service
	if statsdAddr != "" && enabled {
		stats, err = helper.New(true, statsdAddr, statsdType, service, strings.Replace(hostname, ".", "_", -1))
	} else {
		stats, err = helper.New(false, statsdAddr, statsdType, service, strings.Replace(hostname, ".", "_", -1))
	}
	if err != nil {
		log.Fatal(4, "failed to initialize statsd. %s", err)
	}

	flushDuration = stats.NewTimer("metricpublisher.global.flush_duration", 0)

}
