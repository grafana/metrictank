package models

type GraphiteRender struct {
	MaxDataPoints uint32   `json:"maxDataPoints" form:"maxDataPoints"`
	Targets       []string `json:"target" form:"target" binding:"required"`
	From          string   `json:"from" form:"from"`
	Until         string   `json:"until" form:"until"`
	To            string   `json:"to" form:"to"`
	Format        string   `json:"format" form:"format"`
}

type GraphiteFind struct {
	Query  string `json:"query" form:"query" binding:"required"`
	From   int64  `json:"from" form:"from"`
	Until  int64  `json:"until" form:"until"`
	Format string `json:"format" form:"format"`
	Jsonp  string `json:"jsonp" form:"jsonp"`
}

type MetricsDelete struct {
	Query string `json:"query" form:"query" binding:"required"`
}
