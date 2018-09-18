package main

import (
	"flag"
	"fmt"
	"os"
	"runtime"

	"github.com/grafana/metrictank/conf"
	"github.com/grafana/metrictank/consolidation"
	"github.com/grafana/metrictank/logger"
	log "github.com/sirupsen/logrus"
)

var (
	gitHash     = "(none)"
	showVersion = flag.Bool("version", false, "print version string")
	metric      = flag.String("metric", "", "specify a metric name to see which aggregation rule it matches")
)

func main() {
	formatter := &logger.TextFormatter{}
	formatter.TimestampFormat = "2006-01-02 15:04:05.000"
	formatter.QuoteEmptyFields = true

	log.SetFormatter(formatter)
	log.SetLevel(log.InfoLevel)

	flag.Usage = func() {
		fmt.Println("mt-aggs-explain")
		fmt.Println()
		fmt.Println("Usage:")
		fmt.Println()
		fmt.Printf("	mt-aggs-explain [flags] [config-file]\n")
		fmt.Println("           (config file defaults to /etc/metrictank/storage-aggregation.conf)")
		fmt.Println()
		fmt.Println("Flags:")
		flag.PrintDefaults()
	}
	flag.Parse()

	if *showVersion {
		fmt.Printf("mt-aggs-explain (built with %s, git hash %s)\n", runtime.Version(), gitHash)
		return
	}
	if flag.NArg() > 1 {
		flag.Usage()
		os.Exit(-1)
	}
	aggsFile := "/etc/metrictank/storage-aggregation.conf"
	if flag.NArg() == 1 {
		aggsFile = flag.Arg(0)
	}
	aggs, err := conf.ReadAggregations(aggsFile)
	if err != nil {
		log.WithFields(log.Fields{
			"file":  aggsFile,
			"error": err.Error(),
		}).Fatal("can't read aggregations file")
	}

	if *metric != "" {
		aggI, agg := aggs.Match(*metric)
		fmt.Printf("metric %q gets aggI %d\n", *metric, aggI)
		show(agg)
		fmt.Println()
		fmt.Println()
	}

	fmt.Println("### all definitions ###")

	for _, agg := range aggs.Data {
		show(agg)
		fmt.Println()
	}
	fmt.Println("default:")
	fmt.Println(aggs.DefaultAggregation)
}

func show(agg conf.Aggregation) {
	fmt.Println("#", agg.Name)
	fmt.Printf("pattern:   %10s\n", agg.Pattern)
	fmt.Printf("priority:  %10f\n", agg.XFilesFactor)
	fmt.Printf("methods:\n")
	for i, method := range agg.AggregationMethod {
		consolidator := consolidation.Consolidator(method)
		if i == 0 {
			fmt.Println("  ", consolidator.String(), "<-- used for rollup reads and normalization (not: runtime consolidation)")
		} else {
			fmt.Println("  ", consolidator.String())
		}
	}
}
