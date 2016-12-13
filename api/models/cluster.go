package models

import (
	"github.com/raintank/metrictank/idx"
)

//go:generate msgp
type IndexFindResp struct {
	Nodes map[string][]idx.Node
}

//go:generate msgp
func NewIndexFindResp() *IndexFindResp {
	return &IndexFindResp{
		Nodes: make(map[string][]idx.Node),
	}
}

//go:generate msgp
type GetDataResp struct {
	Series []Series
}

type MetricsDeleteResp struct {
	DeletedDefs int `json:"deletedDefs"`
}
