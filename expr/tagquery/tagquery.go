package tagquery

import (
	"flag"

	"github.com/grafana/globalconf"
)

var (
	matchCacheSize int
)

func ConfigSetup() {
	tagQuery := flag.NewFlagSet("tag-query", flag.ExitOnError)
	tagQuery.IntVar(&matchCacheSize, "match-cache-size", 1000, "size of regular expression cache in tag query evaluation")
	globalconf.Register("tag-query", tagQuery, flag.ExitOnError)
}
