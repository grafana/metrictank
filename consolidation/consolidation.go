// Package consolidation provides an abstraction for consolidators
package consolidation

import (
	"errors"

	"github.com/raintank/schema"

	"github.com/grafana/metrictank/batch"
	log "github.com/sirupsen/logrus"
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
	Mult
	Med
	Diff
	StdDev
	Range
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
	case Mult:
		return "MultiplyConsolidator"
	case Med:
		return "MedianConsolidator"
	case Diff:
		return "DifferenceConsolidator"
	case StdDev:
		return "StdDevConsolidator"
	case Range:
		return "RangeConsolidator"
	case Sum:
		return "SumConsolidator"
	}
	log.WithFields(log.Fields{
		"consolidator": c,
	}).Panic("Consolidator.String(): unknown consolidator")
	// This return will never be reached due to the Panic, but Go complains if it is omitted
	return ""
}

// provide the name of a stored archive
// see aggregator.go for which archives are available
func (c Consolidator) Archive() schema.Method {
	switch c {
	case None:
		log.Panic("cannot get an archive for no consolidation")
	case Avg:
		log.Panic("avg consolidator has no matching Archive(). you need sum and cnt")
	case Cnt:
		return schema.Cnt
	case Lst:
		return schema.Lst
	case Min:
		return schema.Min
	case Max:
		return schema.Max
	case Sum:
		return schema.Sum
	}
	log.WithFields(log.Fields{
		"consolidator": c,
	}).Panic("Consolidator.Archive(): unknown consolidator")
	// This return will never be reached due to the Panic, but Go complains if it is omitted
	return schema.Sum
}

func FromArchive(archive schema.Method) Consolidator {
	switch archive {
	case schema.Cnt:
		return Cnt
	case schema.Lst:
		return Lst
	case schema.Min:
		return Min
	case schema.Max:
		return Max
	case schema.Sum:
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
	case "lst", "last", "current":
		return Lst
	case "min":
		return Min
	case "max":
		return Max
	case "mult", "multiply":
		return Mult
	case "med", "median":
		return Med
	case "diff":
		return Diff
	case "stddev":
		return StdDev
	case "range", "rangeOf":
		return Range
	case "sum", "total":
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
	case Mult:
		consFunc = batch.Mult
	case Med:
		consFunc = batch.Med
	case Diff:
		consFunc = batch.Diff
	case StdDev:
		consFunc = batch.StdDev
	case Range:
		consFunc = batch.Range
	case Sum:
		consFunc = batch.Sum
	}
	return consFunc
}

func Validate(fn string) error {
	if fn == "avg" || fn == "average" ||
		fn == "count" ||
		fn == "last" || fn == "current" ||
		fn == "min" ||
		fn == "max" ||
		fn == "mult" || fn == "multiply" ||
		fn == "med" || fn == "median" ||
		fn == "diff" ||
		fn == "stddev" ||
		fn == "range" || fn == "rangeOf" ||
		fn == "sum" || fn == "total" {
		return nil
	}
	return errUnknownConsolidationFunction
}
