package models

import "github.com/grafana/metrictank/pointslicepool"

func init() {
	pointSlicePool = pointslicepool.New(100)
}
