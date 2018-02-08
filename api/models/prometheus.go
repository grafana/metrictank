package models

import (
	"github.com/prometheus/prometheus/promql"
)

type PrometheusQueryRange struct {
	Query   string `form:"query"`   //<string>: Prometheus expression query string.
	Start   string `form:"start"`   //<rfc3339 | unix_timestamp>: Start timestamp.
	End     string `form:"end"`     //<rfc3339 | unix_timestamp>: End timestamp.
	Step    string `form:"step"`    //<duration>: Query resolution step width.
	Timeout string `form:"timeout"` //<duration>: Evaluation timeout. Optional. Defaults to and is capped by the value of the -query.timeout flag.
}

type ProemtheusQueryData struct {
	ResultType promql.ValueType `json:"resultType"`
	Result     promql.Value     `json:"result"`
}
