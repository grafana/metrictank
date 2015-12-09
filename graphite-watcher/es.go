package main

import (
	"encoding/json"
	"github.com/raintank/raintank-metric/schema"
	"io/ioutil"
	"net/http"
)

type Hit struct {
	Index  string                  `json:"_index"`
	Type   string                  `json:"_type"`
	Id     string                  `json:"_id"`
	Score  float64                 `json:"_score"`
	Source schema.MetricDefinition `json:"_source"`
}

type EsResult struct {
	Took     int
	TimedOut bool
	_shards  struct {
		total      int
		successful int
		failed     int
	}
	Hits struct {
		Total    int
		MaxScore int
		Hits     []Hit
	}
}

func getMetrics(host string) []schema.MetricDefinition {
	res, err := http.Get("http://" + host + "/metric/_search?q=*:*&size=10000000")
	perror(err)
	defer res.Body.Close()
	body, err := ioutil.ReadAll(res.Body)
	perror(err)
	var data EsResult
	err = json.Unmarshal(body, &data)
	perror(err)
	amount := len(data.Hits.Hits)
	defs := make([]schema.MetricDefinition, amount)
	for i, h := range data.Hits.Hits {
		defs[i] = h.Source
	}
	return defs
}
