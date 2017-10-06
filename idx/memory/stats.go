package memory

import "github.com/grafana/metrictank/stats"

var (
	// how many times a corrupt index has been detected
	CorruptIndex = stats.NewCounter32("idx.memory.corrupt-index")

	// how many times an invalid tag has been found inside the index
	InvalidTagInIndex = stats.NewCounter32("idx.memory.invalid-tag-in-index")
)
