package main

import (
	"flag"
	"fmt"
	"runtime"
	"time"

	"github.com/grafana/metrictank/logger"
	"github.com/grafana/metrictank/mdata/chunk"
	"github.com/grafana/metrictank/store/cassandra"
	"github.com/raintank/dur"
	log "github.com/sirupsen/logrus"
)

var (
	gitHash     string
	showVersion = flag.Bool("version", false, "print version string")
	spanStr     = flag.String("span", "", "see boundaries for chunks of this span")
	now         = time.Now()
	nowUnix     = now.Unix()
)

func init() {
	formatter := &logger.TextFormatter{}
	formatter.TimestampFormat = "2006-01-02 15:04:05.000"
	formatter.ModuleName = "mt-view-boundaries"
	log.SetFormatter(formatter)
	log.SetLevel(log.InfoLevel)
}

func format(t time.Time) string {
	return t.Format(time.RFC1123Z)
}

func display(span int64, boundaryType string) {
	prevUnix := nowUnix - (nowUnix % span)
	nextUnix := prevUnix + span
	prev := time.Unix(prevUnix, 0)
	next := time.Unix(nextUnix, 0)

	fmt.Println(boundaryType, ":", span, "seconds")
	fmt.Println()
	fmt.Printf("%40s %20s\n", "datetime", "unixts")
	fmt.Printf("%40s %20d <-- prev %s boundary (# %d)\n", format(prev), prevUnix, boundaryType, prevUnix/span)
	fmt.Printf("%40s %20d <-- now\n", format(now), now.Unix())
	fmt.Printf("%40s %20d <-- next %s boundary (# %d)\n", format(next), nextUnix, boundaryType, nextUnix/span)
}

func main() {
	flag.Usage = func() {
		fmt.Println("mt-view-boundaries")
		fmt.Println()
		fmt.Println("Shows boundaries of rows in cassandra and of spans of specified size.")
		fmt.Println("to see UTC times, just prefix command with TZ=UTC")
		fmt.Println()
		flag.PrintDefaults()
	}
	flag.Parse()

	if *showVersion {
		fmt.Printf("mt-view-boundaries (built with %s, git hash %s)\n", runtime.Version(), gitHash)
		return
	}

	display(int64(cassandra.Month_sec), "cassandra rowkey month")

	if *spanStr != "" {
		span := dur.MustParseNDuration("span", *spanStr)
		_, ok := chunk.RevChunkSpans[span]
		if !ok {
			log.Fatalf("chunkSpan %s is not a valid value (https://github.com/grafana/metrictank/blob/master/docs/data-knobs.md#valid-chunk-spans)", *spanStr)
		}
		fmt.Println()
		display(int64(span), "specified span")
	}
}
