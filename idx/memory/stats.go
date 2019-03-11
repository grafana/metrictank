package memory

import "github.com/grafana/metrictank/stats"

var (
	// metric recovered_errors.idx.memory.corrupt-index is how many times
	// a corruption has been detected in one of the internal index structures
	// each time this happens, an error is logged with more details.
	corruptIndex = stats.NewCounter32("recovered_errors.idx.memory.corrupt-index")

	// metric recovered_errors.idx.memory.intern-error is how many times
	// an error is encountered while attempting to intern a string.
	// each time this happens, an error is logged with more details.
	internError = stats.NewCounter32("recovered_errors.idx.memory.intern-error")

	overheadCounter   = stats.NewCounter64("idx.memory.defbytagset-memory")
	mapsizeCounter    = stats.NewCounter64("idx.memory.defbytagset-mapsize")
	mapsizeDefCounter = stats.NewCounter64("idx.memory.defbytagset-defs-mapsize")
)
