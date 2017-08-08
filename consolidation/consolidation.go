// Package consolidation provides an abstraction for consolidators
package consolidation

import (
	"errors"
	"fmt"

	"github.com/raintank/metrictank/batch"
)

// consolidator is a highlevel description of a point consolidation method
// mostly for use by the http api, but can also be used internally for data processing
//go:generate msgp
type Consolidator int

var errUnknownConsolidationFunction = errors.New("unknown consolidation function")

const (
	None Consolidator = iota
	Avg
	Sum
	Lst
	Max
	Min
	Cnt // not available through http api
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
	case Lst:
		return "LastConsolidator"
	case Min:
		return "MinimumConsolidator"
	case Max:
		return "MaximumConsolidator"
	case Sum:
		return "SumConsolidator"
	}
	panic(fmt.Sprintf("Consolidator.String(): unknown consolidator %d", c))
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
	case Lst:
		return "lst"
	case Min:
		return "min"
	case Max:
		return "max"
	case Sum:
		return "sum"
	}
	panic(fmt.Sprintf("Consolidator.Archive(): unknown consolidator %q", c))
}

func FromArchive(archive string) Consolidator {
	switch archive {
	case "cnt":
		return Cnt
	case "lst":
		return Lst
	case "min":
		return Min
	case "max":
		return Max
	case "sum":
		return Sum
	}
	return None
}

func FromConsolidateBy(c string) Consolidator {
	switch c {
	case "avg", "average":
		return Avg
	case "cnt":
		return Cnt // bonus. not supported by graphite
	case "lst", "last":
		return Lst // bonus. not supported by graphite
	case "min":
		return Min
	case "max":
		return Max
	case "sum":
		return Sum
	}
	return None
}

// map the consolidation to the respective aggregation function, if applicable.
func GetAggFunc(consolidator Consolidator) batch.AggFunc {
	var consFunc batch.AggFunc
	switch consolidator {
	case Avg:
		consFunc = batch.Avg
	case Cnt:
		consFunc = batch.Cnt
	case Lst:
		consFunc = batch.Lst
	case Min:
		consFunc = batch.Min
	case Max:
		consFunc = batch.Max
	case Sum:
		consFunc = batch.Sum
	}
	return consFunc
}

func Validate(fn string) error {
	if fn == "avg" || fn == "average" || fn == "last" || fn == "min" || fn == "max" || fn == "sum" {
		return nil
	}
	return errUnknownConsolidationFunction
}
