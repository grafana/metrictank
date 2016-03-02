package main

import (
	"fmt"
	"github.com/raintank/raintank-metric/metricdef"
	"github.com/raintank/raintank-metric/schema"
	"log"
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

func perror(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func main() {
	defs, err := metricdef.NewDefsEs("localhost:9200", "", "", "metric")
	show := func(ds []*schema.MetricDefinition) {
		for _, d := range ds {
			fmt.Println(d.OrgId, d.Name)
		}
	}
	met, scroll_id, err := defs.GetMetrics("")
	perror(err)
	show(met)
	for scroll_id != "" {
		met, scroll_id, err = defs.GetMetrics(scroll_id)
		perror(err)
		show(met)
	}
}
