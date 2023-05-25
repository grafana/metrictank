package expr

import "github.com/grafana/metrictank/internal/pointslicepool"

func init() {
	pointSlicePool = pointslicepool.New(100)
}
