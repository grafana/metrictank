package api

import (
	"github.com/grafana/metrictank/pkg/api/models"
	"github.com/grafana/metrictank/pkg/expr"
	"github.com/grafana/metrictank/pkg/pointslicepool"
)

var pointSlicePool *pointslicepool.PointSlicePool

func init() {
	pointSlicePool = pointslicepool.New(0)
	expr.Pool(pointSlicePool)
	models.Pool(pointSlicePool)
}
