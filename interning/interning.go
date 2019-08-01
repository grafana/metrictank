package interning

import (
	"github.com/grafana/metrictank/stats"
	goi "github.com/robert-milan/go-object-interning"
)

// IdxIntern is a pointer into the object interning layer for the index
//
// Default config does not use compression
var IdxIntern = goi.NewObjectIntern(goi.NewConfig())

// metric recovered_errors.idx.memory.intern-error is how many times
// an error is encountered while attempting to intern a string.
// each time this happens, an error is logged with more details.
var internError = stats.NewCounter32("recovered_errors.idx.memory.intern-error")

// metric recovered_errors.idx.memory.invalid-tag is how many times
// an invalid tag for a metric is encountered.
// each time this happens, an error is logged with more details.
var invalidTag = stats.NewCounter32("recovered_errors.idx.memory.invalid-tag")
