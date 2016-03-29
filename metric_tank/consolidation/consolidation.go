package consolidation

import (
	"fmt"
	"github.com/raintank/raintank-metric/metric_tank/batch"
)

// consolidator is a highlevel description of a point consolidation method
// mostly for use by the http api, but can also be used internally for data processing
type Consolidator uint8

const (
	None Consolidator = iota
	Avg
	Cnt // not available through http api
	Min
	Max
	Sum
)

// String provides human friendly names
func (c Consolidator) String() string {
	switch c {
	case None:
		return "NoneConsolidator"
	case Avg:
		return "AverageConsolidator"
	case Cnt:
		return "CountConsolidator"
	case Min:
		return "MinimumConsolidator"
	case Max:
		return "MaximumConsolidator"
	case Sum:
		return "SumConsolidator"
	}
	panic(fmt.Sprintf("Consolidator.String(): unknown consolidator %v", c))
}

// provide the name of a stored archive
// see aggregator.go for which archives are available
func (c Consolidator) Archive() string {
	switch c {
	case None:
		panic("cannot get an archive for no consolidation")
	case Avg:
		panic("avg consolidator has no matching Archive(). you need sum and cnt")
	case Cnt:
		return "cnt"
	case Min:
		return "min"
	case Max:
		return "max"
	case Sum:
		return "sum"
	}
	panic(fmt.Sprintf("Consolidator.Archive(): unknown consolidator %q", c))
}

// map the consolidation to the respective aggregation function, if applicable.
func GetAggFunc(consolidator Consolidator) batch.AggFunc {
	var consFunc batch.AggFunc
	switch consolidator {
	case Avg:
		consFunc = batch.Avg
	case Cnt:
		consFunc = batch.Cnt
	case Min:
		consFunc = batch.Min
	case Max:
		consFunc = batch.Max
	case Sum:
		consFunc = batch.Sum
	}
	return consFunc
}
