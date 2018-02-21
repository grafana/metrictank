// Package mdata stands for "managed data" or "metrics data" if you will
// it has all the stuff to keep metric data in memory, store it, and synchronize
// save states over the network
package mdata

import (
	"flag"
	"io/ioutil"

	"github.com/grafana/metrictank/conf"
	"github.com/grafana/metrictank/idx"
	"github.com/grafana/metrictank/mdata/cache"
	"github.com/raintank/worldping-api/pkg/log"
	"github.com/rakyll/globalconf"
)

var (
	LogLevel int

	// set either via ConfigProcess or from the unit tests. other code should not touch
	Schemas      conf.Schemas
	Aggregations conf.Aggregations

	schemasFile = "/etc/metrictank/storage-schemas.conf"
	aggFile     = "/etc/metrictank/storage-aggregation.conf"

	BackendStore Store
	MemoryStore  Metrics
	Idx          idx.MetricIndex
	Cache        cache.CachePusher

	DropFirstChunk bool
)

func ConfigSetup() {
	retentionConf := flag.NewFlagSet("retention", flag.ExitOnError)
	retentionConf.StringVar(&schemasFile, "schemas-file", "/etc/metrictank/storage-schemas.conf", "path to storage-schemas.conf file")
	retentionConf.StringVar(&aggFile, "aggregations-file", "/etc/metrictank/storage-aggregation.conf", "path to storage-aggregation.conf file")
	globalconf.Register("retention", retentionConf)
}

func ConfigProcess() {
	var err error

	// === read storage-schemas.conf ===

	// graphite behavior: abort on any config reading errors, but skip any rules that have problems.
	// at the end, add a default schema of 7 days of minutely data.
	// we are stricter and don't tolerate any errors, that seems in the user's best interest.

	Schemas, err = conf.ReadSchemas(schemasFile)
	if err != nil {
		log.Fatal(3, "can't read schemas file %q: %s", schemasFile, err.Error())
	}

	// === read storage-aggregation.conf ===

	// graphite behavior:
	// continue if file can't be read. (e.g. file is optional) but quit if other error reading config
	// always add a default rule with xFilesFactor None and aggregationMethod None
	// (which get interpreted by whisper as 0.5 and avg) at the end.

	// since we can't distinguish errors reading vs parsing, we'll just try a read separately first
	_, err = ioutil.ReadFile(aggFile)
	if err == nil {
		Aggregations, err = conf.ReadAggregations(aggFile)
		if err != nil {
			log.Fatal(3, "can't read storage-aggregation file %q: %s", aggFile, err.Error())
		}
	} else {
		log.Info("Could not read %s: %s: using defaults", aggFile, err)
		Aggregations = conf.NewAggregations()
	}
}
