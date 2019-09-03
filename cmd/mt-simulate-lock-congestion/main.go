package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"
	"time"

	"net/http"
	_ "net/http/pprof"

	"github.com/grafana/globalconf"
	"github.com/grafana/metrictank/cmd/mt-simulate-lock-congestion/runner"
	"github.com/grafana/metrictank/idx/memory"
	"github.com/grafana/metrictank/schema"
)

var (
	queriesFile          = flag.String("queries-file", "", "filename with queries to run")
	seriesFile           = flag.String("series-file", "", "filename with list of series names")
	addsPerSec           = flag.Int("adds-per-sec", 5000, "Metric add operations per second")
	newSeriesPercent     = flag.Int("new-series-percent", 2, "percentage of adds that should be new series")
	addThreads           = flag.Int("add-threads", 8, "Number of threads to concurrently try adding metrics into the index")
	initialIndexSize     = flag.Int("initial-index-size", 1000000, "prepopulate the index with the defined number of metrics before starting the benchmark")
	queriesPerSec        = flag.Int("queries-per-sec", 100, "Index queries per second")
	concQueries          = flag.Int("concurrent-queries", 1000, "Max number of concurrent index queries. (note: this limit does not exist in metrictank)")
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

	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	ctx, cancel := context.WithCancel(context.Background())
	queryGenerator, err := NewQueryGenerator(ctx, *queriesFile)
	if err != nil {
		log.Fatalf("Failed read queries: %s", err.Error())
	}
	metricGenerator, err := NewMetricsGenerator(ctx, *seriesFile, *initialIndexSize, *newSeriesPercent)
	if err != nil {
		log.Fatalf("Failed read series: %s", err.Error())
	}
	testRun := runner.NewTestRun(metricGenerator.Out, queryGenerator.Out, uint32(*addsPerSec), uint32(*addThreads), uint32(*initialIndexSize), uint32(*queriesPerSec), *concQueries)
	go func() {
		ticker := time.NewTicker(time.Second * 2)
		for range ticker.C {
			runtime.GC()
		}
	}()

	testRun.Init(ctx)
	go testRun.Run()

	// TODO: this looks like a duplicate (see above): confirm we can remove this without affecting results
	go func() {
		ticker := time.NewTicker(time.Second * 2)
		for range ticker.C {
			runtime.GC()
		}
	}()
	time.Sleep(*runDuration)
	cancel()
	testRun.Wait()

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

type FileScanner struct {
	scanner *bufio.Scanner
	fh      *os.File
}

func NewFileScanner(filename string) (*FileScanner, error) {
	fh, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("Failed to open file \"%s\": %s", filename, err)
	}
	res := &FileScanner{
		fh:      fh,
		scanner: bufio.NewScanner(fh),
	}
	return res, nil
}

// GetNextLine reads the next line from the file.
// When the end is reached, it wraps back to the beginning.
func (f *FileScanner) GetNextLine() (string, error) {
	ok := f.scanner.Scan()
	if !ok {
		if err := f.scanner.Err(); err != nil {
			return "", err
		}
		_, err := f.fh.Seek(0, 0)
		if err != nil {
			return "", err
		}
		f.scanner = bufio.NewScanner(f.fh)
		ok := f.scanner.Scan()
		if !ok {
			if err := f.scanner.Err(); err != nil {
				return "", err
			}
			return "", fmt.Errorf("no data read from queriesFile")
		}
	}
	return f.scanner.Text(), nil
}

type QueryGenerator struct {
	*FileScanner
	Out chan string
}

func NewQueryGenerator(ctx context.Context, filename string) (*QueryGenerator, error) {
	scanner, err := NewFileScanner(filename)
	if err != nil {
		return nil, err
	}
	res := &QueryGenerator{
		Out:         make(chan string),
		FileScanner: scanner,
	}

	go res.run(ctx)
	return res, nil
}

func (f *QueryGenerator) run(ctx context.Context) {
	for {
		line, err := f.GetNextLine()
		if err != nil {
			log.Fatal(err)
		}
		select {
		case <-ctx.Done():
			log.Printf("queryGenerator loop ending as context is done.")
			close(f.Out)
			return
		case f.Out <- line:
		}

	}
}

type MetricsGenerator struct {
	*FileScanner
	Out              chan *schema.MetricData
	initialIndexSize int
	newSeriesPercent int
}

func NewMetricsGenerator(ctx context.Context, filename string, initialIndexSize, newSeriesPercent int) (*MetricsGenerator, error) {
	scanner, err := NewFileScanner(filename)
	if err != nil {
		return nil, err
	}
	res := &MetricsGenerator{
		FileScanner:      scanner,
		Out:              make(chan *schema.MetricData, 1000),
		initialIndexSize: initialIndexSize,
		newSeriesPercent: newSeriesPercent,
	}

	go res.run(ctx)
	return res, nil
}

func (f *MetricsGenerator) run(ctx context.Context) {
	// note: this may store series rendundantly (multiple times) because the scanner may wrap
	seenSeries := make([]string, 0, f.initialIndexSize*2)
	count := 0
	for {
		point := &schema.MetricData{}
		if count < f.initialIndexSize || count%100 < f.newSeriesPercent || len(seenSeries) == 0 {
			line, err := f.GetNextLine()
			if err != nil {
				log.Fatal(err)
			}
			point = &schema.MetricData{
				OrgId:    1,
				Name:     line,
				Interval: 1,
				Mtype:    "gauge",
				Time:     time.Now().Unix(),
			}
			point.SetId()
			seenSeries = append(seenSeries, line)
		} else {
			line := seenSeries[rand.Intn(len(seenSeries))]
			point = &schema.MetricData{
				OrgId:    1,
				Name:     line,
				Interval: 1,
				Mtype:    "gauge",
				Time:     time.Now().Unix(),
			}
		}
		count++

		select {
		case <-ctx.Done():
			log.Printf("MetricsGenerator loop ending as context is done.")
			close(f.Out)
			return
		case f.Out <- point:
		}
	}
}
