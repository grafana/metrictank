package controlmodels

type IndexDelByQueryReq struct {
	Expr      []string `json:"expr" binding:"Required"`
	OlderThan int64    `json:"olderThan" form:"olderThan"`
	Method    string   `json:"method" binding:";In(dry-run,delete,archive)"`
	Limit     int      `json:"limit" form:"limit"`
}

type IndexDelByQueryResp struct {
	Count      int         `json:"count"`
	Partitions map[int]int `json:"partitions"`
}

type IndexRestoreReq struct {
	Expr          []string `json:"expr" binding:"Required"`
	NumPartitions int      `json:"partitions" binding:"Default(0)"`
}

type IndexRestoreResp struct {
}
