package models

import "github.com/grafana/metrictank/pkg/pointslicepool"

func init() {
	pointSlicePool = pointslicepool.New(100)
}
