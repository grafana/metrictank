package models

import (
	"github.com/grafana/metrictank/pointslicepool"
)

var pointSlicePool *pointslicepool.PointSlicePool

// Pool tells the models package library which pool to use for temporary []schema.Point
func Pool(p *pointslicepool.PointSlicePool) {
	pointSlicePool = p
}
