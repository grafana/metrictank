package api

import (
	"github.com/grafana/metrictank/expr"
	"github.com/grafana/metrictank/pointslicepool"
)

var pointSlicePool *pointslicepool.PointSlicePool

func init() {
	pointSlicePool = pointslicepool.New(pointslicepool.DefaultPointSliceSize)
	expr.Pool(pointSlicePool)
}
