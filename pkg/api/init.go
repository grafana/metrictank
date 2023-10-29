package api

import (
	"github.com/grafana/metrictank/internal/pointslicepool"
	"github.com/grafana/metrictank/pkg/api/models"
	"github.com/grafana/metrictank/pkg/expr"
)

var pointSlicePool *pointslicepool.PointSlicePool

func init() {
	pointSlicePool = pointslicepool.New(0)
	expr.Pool(pointSlicePool)
	models.Pool(pointSlicePool)
}
