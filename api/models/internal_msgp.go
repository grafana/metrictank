package models

import (
	"github.com/raintank/metrictank/idx"
)

//go:generate msgp
type IndexFindResp struct {
	Nodes map[string][]idx.Node
}

func NewIndexFindResp() *IndexFindResp {
	return &IndexFindResp{
		Nodes: make(map[string][]idx.Node),
	}
}

type GetDataResp struct {
	Series []Series
}
