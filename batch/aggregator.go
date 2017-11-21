// Package batch implements batched processing for slices of points
// in particular aggregations
package batch

// aggregation functions for batches of data
import (
	"math"

	"gopkg.in/raintank/schema.v1"
)

type AggFunc func(in []schema.Point) float64

type AggBuilder interface {
	AddPoint(in schema.Point)
	Value() float64
	Reset()
}

type AvgBuilder struct {
	sum float64
	cnt float64
}

func (b *AvgBuilder) AddPoint(in schema.Point) {
	if !math.IsNaN(in.Val) {
		b.sum += in.Val
		b.cnt++
	}
}

func (b *AvgBuilder) Value() float64 {
	if b.cnt > 0 {
		return b.sum / b.cnt
	}
	return math.NaN()
}

func (b *AvgBuilder) Reset() {
	b.sum = 0
	b.cnt = 0
}

func Avg(in []schema.Point) float64 {
	if len(in) == 0 {
		panic("avg() called in aggregator with 0 terms")
	}
	b := AvgBuilder{sum: 0, cnt: 0}
	for _, term := range in {
		b.AddPoint(term)
	}
	return b.Value()
}

type CntBuilder struct {
	cnt float64
}

func (b *CntBuilder) AddPoint(in schema.Point) {
	if !math.IsNaN(in.Val) {
		b.cnt++
	}
}

func (b *CntBuilder) Value() float64 {
	if b.cnt == 0 {
		return math.NaN()
	}
	return b.cnt
}

func (b *CntBuilder) Reset() {
	b.cnt = 0
}

func Cnt(in []schema.Point) float64 {
	b := CntBuilder{cnt: 0}
	for _, v := range in {
		b.AddPoint(v)
	}
	return b.Value()
}

type LstBuilder struct {
	lst float64
}

func (b *LstBuilder) AddPoint(in schema.Point) {
	if !math.IsNaN(in.Val) {
		b.lst = in.Val
	}
}

func (b *LstBuilder) Value() float64 {
	return b.lst
}

func (b *LstBuilder) Reset() {
	b.lst = math.NaN()
}

func Lst(in []schema.Point) float64 {
	if len(in) == 0 {
		panic("last() called in aggregator with 0 terms")
	}
	b := LstBuilder{lst: math.NaN()}
	for _, v := range in {
		b.AddPoint(v)
	}
	return b.Value()
}

type MinBuilder struct {
	min float64
}

func (b *MinBuilder) AddPoint(in schema.Point) {
	if !math.IsNaN(in.Val) {
		if math.IsNaN(b.min) || in.Val < b.min {
			b.min = in.Val
		}
	}
}

func (b *MinBuilder) Value() float64 {
	return b.min
}

func (b *MinBuilder) Reset() {
	b.min = math.NaN()
}

func Min(in []schema.Point) float64 {
	if len(in) == 0 {
		panic("min() called in aggregator with 0 terms")
	}
	b := MinBuilder{min: math.NaN()}
	for _, v := range in {
		b.AddPoint(v)
	}
	return b.Value()
}

type MaxBuilder struct {
	max float64
}

func (b *MaxBuilder) AddPoint(in schema.Point) {
	if !math.IsNaN(in.Val) {
		if math.IsNaN(b.max) || in.Val > b.max {
			b.max = in.Val
		}
	}
}

func (b *MaxBuilder) Value() float64 {
	return b.max
}

func (b *MaxBuilder) Reset() {
	b.max = math.NaN()
}

func Max(in []schema.Point) float64 {
	if len(in) == 0 {
		panic("max() called in aggregator with 0 terms")
	}
	b := MaxBuilder{max: math.NaN()}
	for _, v := range in {
		b.AddPoint(v)
	}
	return b.Value()
}

type SumBuilder struct {
	sum float64
}

func (b *SumBuilder) AddPoint(in schema.Point) {
	if !math.IsNaN(in.Val) {
		if math.IsNaN(b.sum) {
			b.sum = 0
		}
		b.sum += in.Val
	}
}

func (b *SumBuilder) Value() float64 {
	return b.sum
}

func (b *SumBuilder) Reset() {
	b.sum = math.NaN()
}

func Sum(in []schema.Point) float64 {
	b := SumBuilder{sum: math.NaN()}
	for _, term := range in {
		b.AddPoint(term)
	}
	return b.Value()
}

// map the consolidation to the respective aggregation function, if applicable.
func GetAggFunc(name string) AggBuilder {
	switch name {
	case "avg", "average":
		return &AvgBuilder{sum: 0, cnt: 0}
	case "cnt":
		return &CntBuilder{cnt: 0}
	case "lst", "last":
		return &LstBuilder{lst: math.NaN()}
	case "min":
		return &MinBuilder{min: math.NaN()}
	case "max":
		return &MaxBuilder{max: math.NaN()}
	case "sum":
		return &SumBuilder{sum: math.NaN()}
	}
	return nil
}
