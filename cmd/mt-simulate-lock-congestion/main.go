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

	"github.com/grafana/metrictank/cmd/mt-simulate-lock-congestion/metricname"

	"github.com/grafana/globalconf"
	"github.com/grafana/metrictank/cmd/mt-simulate-lock-congestion/runner"
	"github.com/grafana/metrictank/idx/memory"
)

var (
	nameGeneratorType    = flag.String("name-generator", "increasing-number", "select name generator (increasing-number|file)")
	nameGeneratorArgs    = flag.String("name-generator-args", "", "args to pass to the name generator")
	addsPerSec           = flag.Int("adds-per-sec", 100000, "Metric add operations per second")
	addThreads           = flag.Int("add-threads", 10, "Number of threads to concurrently try adding metrics into the index")
	addSampleFactor      = flag.Int("add-sample-factor", 100000, "how often to print a sample metric name that we added")
	initialIndexSize     = flag.Int("initial-index-size", 1000000, "prepopulate the index with the defined number of metrics before starting the benchmark")
	queriesPerSec        = flag.Int("queries-per-sec", 100, "Index queries per second")
	querySampleFactor    = flag.Int("query-sample-factor", 100, "how often to print a sample query")
	runDuration          = flag.Duration("run-duration", time.Second*10, "How long we want the test to run")
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

	var nameGenerator metricname.NameGenerator
	switch *nameGeneratorType {
	case "increasingNumber":
		nameGenerator = metricname.NewIncreasingNumberGenerator()
	case "file":
		if len(*nameGeneratorArgs) == 0 {
			log.Fatalf("Requiring the file name")
		}
		nameGenerator, err = metricname.NewFileGenerator(*nameGeneratorArgs)
	default:
		log.Fatalf("Unknown name generator: %s", *nameGeneratorType)
	}
	if err != nil {
		log.Fatalf("Error when instantiating name generator: %s", err)
	}

	testRun := runner.NewTestRun(nameGenerator, uint32(*addsPerSec), uint32(*addThreads), uint32(*addSampleFactor), uint32(*initialIndexSize), uint32(*queriesPerSec), uint32(*querySampleFactor), *runDuration)
	testRun.Run()
	testRun.PrintStats()

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
