package main

import (
	"flag"
	"fmt"
	"github.com/raintank/raintank-metric/dur"
	"github.com/raintank/raintank-metric/metricdef"
	"github.com/raintank/raintank-metric/schema"
	"log"
	"math/rand"
	"os"
	"strings"
	"time"
)

func perror(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

var esAddr = flag.String("es-addr", "localhost:9200", "elasticsearch address")
var esIndex = flag.String("es-index", "metric", "elasticsearch index to query")
var format = flag.String("format", "list", "format: list|vegeta-graphite|vegeta-mt|vegeta-mt-graphite")
var maxAge = flag.Int("max-age", 23400, "max age (last update diff with now) of metricdefs. defaults to 6.5hr. use 0 to disable")
var from = flag.String("from", "30min", "from. eg '30min', '5h', '14d', etc")
var silent = flag.Bool("silent", false, "silent mode (don't print number of metrics loaded to stderr)")
var fromS uint32
var total int

func showList(ds []*schema.MetricDefinition) {
	for _, d := range ds {
		if *maxAge != 0 && d.LastUpdate > time.Now().Unix()-int64(*maxAge) {
			total += 1
			fmt.Println(d.OrgId, d.Name)
		}
	}
}
func showVegetaGraphite(ds []*schema.MetricDefinition) {
	for _, d := range ds {
		if *maxAge != 0 && d.LastUpdate > time.Now().Unix()-int64(*maxAge) {
			total += 1
			fmt.Printf("GET http://localhost:8888/render?target=%s&from=-%s\nX-Org-Id: %d\n\n", d.Name, *from, d.OrgId)
		}
	}
}

func showVegetaMT(ds []*schema.MetricDefinition) {
	from := time.Now().Add(-time.Duration(fromS) * time.Second)
	for _, d := range ds {
		if *maxAge != 0 && d.LastUpdate > time.Now().Unix()-int64(*maxAge) {
			total += 1
			fmt.Printf("GET http://localhost:6063/get?target=%s&from=%d\n", d.Id, from.Unix())
		}
	}
}

func showVegetaMTGraphite(ds []*schema.MetricDefinition) {
	for _, d := range ds {
		if *maxAge != 0 && d.LastUpdate > time.Now().Unix()-int64(*maxAge) {
			total += 1
			mode := rand.Intn(3)
			name := d.Name
			if mode == 0 {
				// in this mode, replaces a node with a dot
				which := rand.Intn(d.NodeCount)
				parts := strings.Split(d.Name, ".")
				parts[which] = "*"
				name = strings.Join(parts, ".")
			} else if mode == 1 {
				// randomly replace chars with a *
				// note that in 1/5 cases, nothing happens
				// and otherwise, sometimes valid patterns are produced,
				// but it's also possible to produce patterns that won't match anything (if '.' was taken out)
				chars := rand.Intn(5)
				pos := rand.Intn(len(d.Name) - chars)
				name = name[0:pos] + "*" + name[pos+chars:]
			}
			// mode 3: do nothing :)

			fmt.Printf("GET http://localhost:6063/render?target=%s&from=-%s\nX-Org-Id: %d\n\n", name, *from, d.OrgId)
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
	case "vegeta-mt-graphite":
		show = showVegetaMTGraphite
	default:
		log.Fatal("invalid format")
	}
	var err error
	fromS, err = dur.ParseUNsec(*from)
	perror(err)
	defs, err := metricdef.NewDefsEs(*esAddr, "", "", *esIndex)
	perror(err)
	met, scroll_id, err := defs.GetMetrics("")
	perror(err)
	show(met)
	for scroll_id != "" {
		met, scroll_id, err = defs.GetMetrics(scroll_id)
		perror(err)
		show(met)
	}
	if !*silent {
		fmt.Fprintf(os.Stderr, "listed %d metrics\n", total)
	}
}
