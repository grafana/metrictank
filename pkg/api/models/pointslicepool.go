package models

import (
	"github.com/grafana/metrictank/pkg/pointslicepool"
)

var pointSlicePool *pointslicepool.PointSlicePool

func init() {
	pointSlicePool = pointslicepool.New(0)
}

// Pool tells the models package library which pool to use for temporary []schema.Point
func Pool(p *pointslicepool.PointSlicePool) {
	pointSlicePool = p
}
