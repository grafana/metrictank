package consolidation

import (
	"github.com/raintank/raintank-metric/metric_tank/batch"
)

// consolidator is a highlevel description of a point consolidation method
// mostly for use by the http api, but can also be used internally for data processing
type Consolidator int

const (
	None Consolidator = iota
	Avg
	Cnt // not available through http api
	Last
	Min
	Max
	Sum
)

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
	case Last:
		return "lst"
	case Min:
		return "min"
	case Max:
		return "max"
	case Sum:
		return "sum"
	}
	panic("unknown consolidator to String()")
}

// map the consolidation to the respective aggregation function, if applicable.
func GetAggFunc(consolidator Consolidator) batch.AggFunc {
	var consFunc batch.AggFunc
	switch consolidator {
	case Avg:
		consFunc = batch.Avg
	case Cnt:
		consFunc = batch.Cnt
	case Last:
		consFunc = batch.Last
	case Min:
		consFunc = batch.Min
	case Max:
		consFunc = batch.Max
	case Sum:
		consFunc = batch.Sum
	}
	return consFunc
}
