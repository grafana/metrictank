package main

import (
	"encoding/json"
	"fmt"
	"github.com/raintank/raintank-metric/schema"
	"io/ioutil"
	"log"
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

func perror(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func main() {
	res, err := http.Get("http://localhost:9200/metric/_search?q=*:*&size=10000000")
	perror(err)
	defer res.Body.Close()
	body, err := ioutil.ReadAll(res.Body)
	perror(err)
	var data EsResult
	err = json.Unmarshal(body, &data)
	perror(err)
	for _, h := range data.Hits.Hits {
		fmt.Println(h.Source.Name)
	}
}
