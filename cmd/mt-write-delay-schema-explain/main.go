package main

import (
	"flag"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"time"

	"github.com/grafana/metrictank/conf"
	"github.com/grafana/metrictank/logger"
	"github.com/grafana/metrictank/util/align"
	log "github.com/sirupsen/logrus"
)

var (
	version      = "(none)"
	showVersion  = flag.Bool("version", false, "print version string")
	windowFactor = flag.Int("window-factor", 20, "size of compaction window relative to TTL")
	interval     = flag.Int("int", 0, "specify an interval to apply interval-based matching in addition to metric matching (e.g. to simulate kafka-mdm input)")
)

func init() {
	formatter := &logger.TextFormatter{}
	formatter.TimestampFormat = "2006-01-02 15:04:05.000"
	log.SetFormatter(formatter)
	log.SetLevel(log.InfoLevel)
}

func main() {
	flag.Usage = func() {
		fmt.Println("mt-write-delay-schema-explain")
		fmt.Println("use this tool to diagnose which retentions may be subject to chunks not showing, if your write queue (temporarily) doesn't drain well")
		fmt.Println("if the following applies to you:")
		fmt.Println("* metrictank is struggling to clear the write queue (e.g. cassandra/bigtable has become slow)")
		fmt.Println("* metrictank only keeps a limited amount of chunks in its ringbuffers (tank)")
		fmt.Println("* you are concerned that chunks needed to satisfy read queries, are not available in the chunk store")
		fmt.Println("then this tool will help you understand, which is especially useful if you have multiple retentions with different chunkspans and/or numchunks")
		fmt.Println("Usage:")
		fmt.Println()
		fmt.Printf("	mt-write-delay-schema-explain [flags] last-drain-time [schemas-file]\n")
		fmt.Println("           (config file defaults to /etc/metrictank/storage-schemas.conf)")
		fmt.Println("           last-drain-time is the last time the queue was empty")
		fmt.Println()
		fmt.Println("Flags:")
		flag.PrintDefaults()
	}
	flag.Parse()

	if *showVersion {
		fmt.Printf("mt-write-delay-schema-explain (version: %s - runtime: %s)\n", version, runtime.Version())
		return
	}
	if flag.NArg() < 1 || flag.NArg() > 2 {
		flag.Usage()
		os.Exit(-1)
	}
	lastDrain, err := strconv.Atoi(flag.Arg(0))
	if err != nil {
		log.Fatalf("cannot parse last-drain time %q: %s", flag.Arg(0), err.Error())
	}
	schemasFile := "/etc/metrictank/storage-schemas.conf"
	if flag.NArg() == 2 {
		schemasFile = flag.Arg(1)
	}
	schemas, err := conf.ReadSchemas(schemasFile)
	if err != nil {
		log.Fatalf("can't read schemas file %q: %s", schemasFile, err.Error())
	}

	s, def := schemas.ListRaw()
	now := time.Now()

	for _, schema := range s {
		display(schema, uint32(lastDrain), uint32(now.Unix()))
	}
	fmt.Println("built-in default:")
	display(def, uint32(lastDrain), uint32(now.Unix()))
}

func showTime(t uint32) string {
	return time.Unix(int64(t), 0).UTC().Format("2006-01-02 15:04:05")
}
func display(schema conf.Schema, lastDrain, now uint32) {
	fmt.Println("#", schema.Name)
	fmt.Printf("pattern:   %10s\n", schema.Pattern)
	fmt.Printf("priority:  %10d\n", schema.Priority)
	for _, ret := range schema.Retentions.Rets {

		savedUntil := align.Backward(lastDrain, ret.ChunkSpan) // note: exclusive

		inMemorySince := align.Backward(now, ret.ChunkSpan)
		if ret.NumChunks > 1 {
			inMemorySince -= (ret.NumChunks - 1) * ret.ChunkSpan
		}

		if inMemorySince > savedUntil {
			fmt.Printf("%20s missing data %s - %s (last drain %s)\n", ret.String(), showTime(savedUntil), showTime(inMemorySince), showTime(lastDrain))
		} else {
			fmt.Printf("%20s OK\n", ret.String())
		}
	}
	fmt.Println()
}
