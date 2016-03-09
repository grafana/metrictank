package main

import (
	"flag"
	"fmt"
	"github.com/raintank/raintank-metric/metricdef"
	"github.com/raintank/raintank-metric/schema"
	"log"
	"os"
	"time"
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

var esAddr = flag.String("es-addr", "localhost:9200", "elasticsearch address")
var esIndex = flag.String("es-index", "metrictank", "elasticsearch index to query")
var format = flag.String("format", "list", "format: list|vegeta-graphite|vegeta-mt")
var maxAge = flag.Int("max-age", 3600, "max age (last update diff with now) of metricdefs")
var from = flag.String("from", "30min", "from. eg '30min', '5h', '14d', etc")
var silent = flag.Bool("silent", false, "silent mode (don't print number of metrics loaded to stderr)")
var fromS uint32

func showList(ds []*schema.MetricDefinition) {
	for _, d := range ds {
		fmt.Println(d.OrgId, d.Name)
	}
}
func showVegetaGraphite(ds []*schema.MetricDefinition) {
	for _, d := range ds {
		fmt.Printf("GET http://localhost:8888/render?target=%s&from=-%s\nX-Org-Id: %d\n", d.Name, *from, d.OrgId)
	}
}
func showVegetaMT(ds []*schema.MetricDefinition) {
	from := time.Now().Add(-time.Duration(fromS) * time.Second)
	for _, d := range ds {
		if d.LastUpdate > time.Now().Unix()-int64(*maxAge) {
			fmt.Printf("GET http://localhost:18763/get?target=%s&from=%d\n", d.Id, from.Unix())
		}
	}
}

func main() {
	flag.Parse()
	var show func(ds []*schema.MetricDefinition)
	switch *format {
	case "list":
		show = showList
	case "vegeta-graphite":
		show = showVegetaGraphite
	case "vegeta-mt":
		show = showVegetaMT
	default:
		log.Fatal("invalid format")
	}
	var err error
	fromS, err = inSeconds(*from)
	perror(err)
	defs, err := metricdef.NewDefsEs(*esAddr, "", "", *esIndex)
	perror(err)
	met, scroll_id, err := defs.GetMetrics("")
	perror(err)
	total := len(met)
	show(met)
	for scroll_id != "" {
		met, scroll_id, err = defs.GetMetrics(scroll_id)
		perror(err)
		show(met)
		total += len(met)
	}
	if !*silent {
		fmt.Fprintf(os.Stderr, "listed %d metrics\n", total)
	}
}
