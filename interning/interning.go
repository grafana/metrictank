package interning

import (
	"flag"

	"github.com/grafana/globalconf"
	"github.com/grafana/metrictank/stats"
	goi "github.com/robert-milan/go-object-interning"
)

var (
	// Compression is a flag used to turn compression on or off
	// in the interning layer
	Compression bool

	// IdxIntern is a pointer into the object interning layer for the index
	//
	// Default config does not use compression
	IdxIntern = goi.NewObjectIntern(goi.NewConfig())

	// metric recovered_errors.idx.memory.intern-error is how many times
	// an error is encountered while attempting to intern a string.
	// each time this happens, an error is logged with more details.
	internError = stats.NewCounter32("recovered_errors.idx.memory.intern-error")

	// metric recovered_errors.idx.memory.invalid-tag is how many times
	// an invalid tag for a metric is encountered.
	// each time this happens, an error is logged with more details.
	invalidTag = stats.NewCounter32("recovered_errors.idx.memory.invalid-tag")
)

func ConfigSetup() {
	intern := flag.NewFlagSet("intern", flag.ExitOnError)
	intern.BoolVar(&Compression, "compression", false, "")
	globalconf.Register("intern", intern, flag.ExitOnError)
}

func ConfigProcess() {
	c := goi.NewConfig()
	if Compression {
		c.Compression = goi.Shoco
	}

	IdxIntern = goi.NewObjectIntern(c)
}
