package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/grafana/globalconf"
	"github.com/grafana/metrictank/cmd/mt-simulate-lock-congestion/runner"
	"github.com/grafana/metrictank/idx/memory"
)

var (
	addsPerSec    = flag.Int("adds-per-sec", 100, "Metric add operations per second")
	addThreads    = flag.Int("add-threads", 8, "Number of threads to concurrently try adding metrics into the index")
	queriesPerSec = flag.Int("queries-per-sec", 100, "Index queries per second")
	queryThreads  = flag.Int("query-threads", 2, "Number of threads to concurrently query the index")
	runDuration   = flag.Duration("run-duration", time.Minute, "How long we want the test to run")
)

func main() {
	config, err := globalconf.NewWithOptions(&globalconf.Options{
		Filename:  "",
		EnvPrefix: "MT_",
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: configuration file error: %s", err)
		os.Exit(1)
	}

	memory.ConfigSetup()
	config.ParseAll()
	memory.ConfigProcess()

	testRun := runner.NewTestRun(uint32(*addsPerSec), uint32(*addThreads), uint32(*queriesPerSec), uint32(*queryThreads), *runDuration)
	testRun.Run()
}
