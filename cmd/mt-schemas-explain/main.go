package main

import (
	"flag"
	"fmt"
	"os"
	"runtime"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/grafana/metrictank/conf"
	"github.com/grafana/metrictank/mdata"
)

var (
	gitHash      = "(none)"
	showVersion  = flag.Bool("version", false, "print version string")
	windowFactor = flag.Int("window-factor", 20, "size of compaction window relative to TTL")
	metric       = flag.String("metric", "", "specify a metric name to see which schema it matches")
	interval     = flag.Int("int", 0, "specify an interval to apply interval-based matching in addition to metric matching (e.g. to simulate kafka-mdm input)")
)

func main() {
	flag.Usage = func() {
		fmt.Println("mt-schemas-explain")
		fmt.Println()
		fmt.Println("Usage:")
		fmt.Println()
		fmt.Printf("	mt-schemas-explain [flags] [config-file]\n")
		fmt.Println("           (config file defaults to /etc/metrictank/storage-schemas.conf)")
		fmt.Println()
		fmt.Println("Flags:")
		flag.PrintDefaults()
	}
	flag.Parse()

	if *showVersion {
		fmt.Printf("mt-schemas-explain (built with %s, git hash %s)\n", runtime.Version(), gitHash)
		return
	}
	if flag.NArg() > 1 {
		flag.Usage()
		os.Exit(-1)
	}
	schemasFile := "/etc/metrictank/storage-schemas.conf"
	if flag.NArg() == 1 {
		schemasFile = flag.Arg(0)
	}
	schemas, err := conf.ReadSchemas(schemasFile)
	if err != nil {
		log.Fatalf("can't read schemas file %q: %s", schemasFile, err.Error())
	}

	if *metric != "" {
		schemaI, s := schemas.Match(*metric, *interval)
		fmt.Printf("metric %q with interval %d gets schemaI %d\n", *metric, *interval, schemaI)
		fmt.Printf("## [%q] pattern=%q prio=%d retentions=%v\n", s.Name, s.Pattern, s.Priority, s.Retentions)
		fmt.Println()
	}

	for _, schema := range schemas.List() {
		fmt.Println("#", schema.Name)
		fmt.Printf("pattern:   %10s\n", schema.Pattern)
		fmt.Printf("priority:  %10d\n", schema.Priority)
		fmt.Printf("retentions:%10s %10s %10s %10s %10s %15s %10s\n", "interval", "retention", "chunkspan", "numchunks", "ready", "tablename", "windowsize")
		for _, ret := range schema.Retentions {
			retention := ret.MaxRetention()
			table := mdata.GetTTLTable(uint32(retention), *windowFactor, mdata.Table_name_format)
			retStr := time.Duration(time.Duration(retention) * time.Second).String()
			if retention%(3600*24) == 0 {
				retStr = fmt.Sprintf("%dd", retention/3600/24)
			}
			chunkSpanStr := time.Duration(time.Duration(ret.ChunkSpan) * time.Second).String()
			windowSizeStr := time.Duration(time.Duration(table.WindowSize) * time.Hour).String()
			fmt.Printf("           %10d %10s %10s %10d %10t %15s %10s\n", ret.SecondsPerPoint, retStr, chunkSpanStr, ret.NumChunks, ret.Ready, table.Table, windowSizeStr)
		}
		fmt.Println()
	}
}
