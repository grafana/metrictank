package api

import (
	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/expr"
	"github.com/grafana/metrictank/pointslicepool"
)

var pointSlicePool *pointslicepool.PointSlicePool

func init() {
	pointSlicePool = pointslicepool.New(int(minSliceSize))
	expr.Pool(pointSlicePool)
	models.Pool(pointSlicePool)
}
