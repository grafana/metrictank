package archive

import (
	"github.com/raintank/metrictank/mdata/chunk"
	"gopkg.in/raintank/schema.v1"
)

//go:generate msgp
type Archive struct {
	RowKey          string
	SecondsPerPoint uint32
	Points          uint32
	Chunks          []chunk.IterGen
}

//go:generate msgp
type Metric struct {
	MetricData        schema.MetricData
	AggregationMethod uint32
	Archives          []Archive
}
