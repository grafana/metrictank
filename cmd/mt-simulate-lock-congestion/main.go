package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"
	"time"

	"github.com/grafana/globalconf"
	"github.com/grafana/metrictank/cmd/mt-simulate-lock-congestion/runner"
	"github.com/grafana/metrictank/idx/memory"
)

var (
	addsPerSec           = flag.Int("adds-per-sec", 100, "Metric add operations per second")
	addThreads           = flag.Int("add-threads", 8, "Number of threads to concurrently try adding metrics into the index")
	queriesPerSec        = flag.Int("queries-per-sec", 100, "Index queries per second")
	queryThreads         = flag.Int("query-threads", 2, "Number of threads to concurrently query the index")
	runDuration          = flag.Duration("run-duration", time.Minute, "How long we want the test to run")
	profileNamePrefix    = flag.String("profile-name-prefix", "profile", "Prefix to prepend before profile file names")
	blockProfileRate     = flag.Int("block-profile-rate", 0, "Sampling rate of block profile, 0 means disabled")
	mutexProfileFraction = flag.Int("mutex-profile-rate", 0, "Fraction of mutex samples, 0 means disabled")
	cpuProfile           = flag.Bool("cpu-profile", false, "Enable cpu profile")
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

	runtime.SetBlockProfileRate(*blockProfileRate)
	runtime.SetMutexProfileFraction(*mutexProfileFraction)
	if *cpuProfile {
		filenamePrefix := *profileNamePrefix + ".cpu."
		f, err := os.Create(findFreeFileName(filenamePrefix))
		if err != nil {
			log.Fatalf("Failed to create cpu profile: %s", err.Error())
		}
		defer f.Close()
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatal("could not start cpu profile: ", err)
		}
		defer pprof.StopCPUProfile()
	}

	testRun := runner.NewTestRun(uint32(*addsPerSec), uint32(*addThreads), uint32(*queriesPerSec), uint32(*queryThreads), *runDuration)
	testRun.Run()

	if *blockProfileRate > 0 {
		filenamePrefix := *profileNamePrefix + ".block."
		f, err := os.Create(findFreeFileName(filenamePrefix))
		if err != nil {
			log.Fatalf("Failed to create block profile: %s", err.Error())
		}
		defer f.Close()
		err = pprof.Lookup("block").WriteTo(f, 0)
		if err != nil {
			log.Fatalf("Failed to write into block profile: %s", err.Error())
		}
	}

	if *mutexProfileFraction > 0 {
		filenamePrefix := *profileNamePrefix + ".mutex."
		f, err := os.Create(findFreeFileName(filenamePrefix))
		if err != nil {
			log.Fatalf("Failed to create mutex profile: %s", err.Error())
		}
		defer f.Close()
		err = pprof.Lookup("mutex").WriteTo(f, 0)
		if err != nil {
			log.Fatalf("Failed to write into mutex profile: %s", err.Error())
		}
	}

}

func findFreeFileName(prefix string) string {
	for profileID := 0; profileID < 1000; profileID++ {
		filename := prefix + strconv.Itoa(profileID)
		_, err := os.Stat(filename)
		if err != nil {
			return filename
		}
	}

	log.Fatalf("unable to find free filename for prefix \"%s\"", prefix)
	return ""
}
