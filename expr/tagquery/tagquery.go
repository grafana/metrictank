package tagquery

import (
	"flag"

	"github.com/grafana/globalconf"
)

var (
	matchCacheSize int
	metaTagSupport bool
)

func ConfigSetup() {
	tagQuery := flag.NewFlagSet("tag-query", flag.ExitOnError)
	tagQuery.IntVar(&matchCacheSize, "match-cache-size", 1000, "size of regular expression cache in tag query evaluation")
	tagQuery.BoolVar(&metaTagSupport, "meta-tag-support", false, "enables/disables querying based on meta tags which get defined via meta tag rules")
	globalconf.Register("tag-query", tagQuery, flag.ExitOnError)
}
