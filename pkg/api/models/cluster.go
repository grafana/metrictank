package models

import (
	"github.com/grafana/metrictank/idx"
)

//go:generate msgp
type StringList []string

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
type GetDataRespV0 struct {
	Series []Series
}

//go:generate msgp
type GetDataRespV1 struct {
	Stats  StorageStats
	Series []Series
}

type MetricsDeleteResp struct {
	DeletedDefs int `json:"deletedDefs"`
}

//go:generate msgp
type IndexTagsResp struct {
	Tags []string `json:"tags"`
}

//go:generate msgp
type IndexTagDetailsResp struct {
	Values map[string]uint64 `json:"values"`
}

//go:generate msgp
type IndexFindByTagResp struct {
	Metrics []idx.Node `json:"metrics"`
}

//go:generate msgp
type IndexTagDelSeriesResp struct {
	Count int
}

//go:generate msgp
type IndexTagDelByQueryResp struct {
	Count int
}
