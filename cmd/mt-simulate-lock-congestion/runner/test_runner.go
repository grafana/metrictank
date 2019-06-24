package runner

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/grafana/metrictank/cmd/mt-simulate-lock-congestion/metricname"
	"github.com/grafana/metrictank/cmd/mt-simulate-lock-congestion/query"

	"github.com/grafana/metrictank/idx"
	"github.com/grafana/metrictank/idx/memory"
	"github.com/raintank/schema"
	"golang.org/x/sync/errgroup"
)

// TestRun represents one test run, including all the structures used for the test
type TestRun struct {
	index                 idx.MetricIndex
	metricNameGenerator   metricname.NameGenerator
	queryPatternGenerator query.QueryGenerator
	stats                 runStats
	addsPerSec            uint32 // how many adds per second we want to execute. 0 means unlimited, as many as possible
	addThreads            uint32 // number of concurrent add threads
	addSampleFactor       uint32 // how often to print info about a completed add
	addDelay              uint32 // once the benchmark starts the querying threads start, if addDelay is >0 then the adding into the index is delayed by the given number of seconds
	initialIndexSize      uint32 // prepopulate the index with the defined number of entries before starting the actual test run
	queriesPerSec         uint32 // how many queries per second we want to execute. 0 means unlimited, as many as possible
	querySampleFactor     uint32 // how often to print info about a completed query
	totalQueryCount       uint32 // the total number of queries we will start
	runDuration           time.Duration
}

const orgID = 1

// NewTestRun Instantiates a new test run
func NewTestRun(nameGenerator metricname.NameGenerator, queryGenerator query.QueryGenerator, addDelay, addsPerSec, addThreads, addSampleFactor, initialIndexSize, queriesPerSec, querySampleFactor uint32, runDuration time.Duration) *TestRun {
	totalQueryCount := queriesPerSec * uint32(runDuration.Seconds())

	runner := TestRun{
		stats:                 newRunStats(totalQueryCount),
		totalQueryCount:       totalQueryCount,
		index:                 memory.New(),
		metricNameGenerator:   nameGenerator,
		queryPatternGenerator: queryGenerator,
		addsPerSec:            addsPerSec,
		addThreads:            addThreads,
		addSampleFactor:       addSampleFactor,
		addDelay:              addDelay,
		initialIndexSize:      initialIndexSize,
		queriesPerSec:         queriesPerSec,
		querySampleFactor:     querySampleFactor,
		runDuration:           runDuration,
	}

	return &runner
}

// Run executes the run
func (t *TestRun) Run() {
	mainCtx, cancel := context.WithCancel(context.Background())

	log.Printf("Starting the metric name generator")
	t.metricNameGenerator.Start(mainCtx, t.addThreads)

	log.Printf("Starting the query generator")
	t.queryPatternGenerator.Start()

	log.Printf("Prepopulating the index with %d entries", t.initialIndexSize)
	t.prepopulateIndex()

	// wait group to wait until all routines have been started
	startedWg := sync.WaitGroup{}
	workerThreads, workerCtx := errgroup.WithContext(mainCtx)

	startedWg.Add(1)
	workerThreads.Go(t.queryRoutine(workerCtx, &startedWg))

	<-time.After(time.Second * time.Duration(t.addDelay))

	startedWg.Add(int(t.addThreads))
	for i := uint32(0); i < t.addThreads; i++ {
		workerThreads.Go(t.addRoutine(workerCtx, &startedWg, i))
	}
	startedWg.Wait()
	log.Printf("Benchmark has started")

	// as soon as all routines have started, we start the timer
	<-time.After(t.runDuration)
	cancel()
	workerThreads.Wait()
}

// PrintStats writes all the statistics in human readable format into stdout
func (t *TestRun) PrintStats() {
	t.stats.Print(uint32(t.runDuration.Seconds()))
}

func (t *TestRun) prepopulateIndex() {
	for i := uint32(0); i < t.initialIndexSize; i++ {
		md := schema.MetricData{OrgId: orgID, Interval: 1, Value: 2, Time: int64(time.Now().Unix()), Mtype: "gauge"}
		md.Name = t.metricNameGenerator.GetNewMetricName()
		md.SetId()
		key, err := schema.MKeyFromString(md.Id)
		if err != nil {
			log.Fatalf("Unexpected error when generating MKey from ID string: %s", err)
		}

		t.index.AddOrUpdate(key, &md, int32(i%t.addThreads))
	}
}

func (t *TestRun) addRoutine(ctx context.Context, startedWg *sync.WaitGroup, routineID uint32) func() error {
	partitionID := int32(routineID)

	addEntryToIndex := func(time int) error {
		md := schema.MetricData{OrgId: orgID, Interval: 1, Value: 2, Time: int64(time), Mtype: "gauge"}
		md.Name = t.metricNameGenerator.GetNewMetricName()
		md.SetId()
		key, err := schema.MKeyFromString(md.Id)
		if err != nil {
			return fmt.Errorf("Unexpected error when generating MKey from ID string: %s", err)
		}

		t.index.AddOrUpdate(key, &md, partitionID)
		adds := t.stats.incAddsCompleted()
		if adds%t.addSampleFactor == 0 {
			log.Printf("Sample: added metric name to index %s", md.Name)
		}

		return nil
	}

	if t.addsPerSec > 0 {
		interval := time.Duration(int64(1000000000) * int64(t.addThreads) / int64(t.addsPerSec))

		return func() error {
			ticker := time.NewTicker(interval)
			go func() {
				<-ctx.Done()
				ticker.Stop()
			}()

			startedWg.Done()
			for {
				select {
				case <-ctx.Done():
					return nil
				case tick := <-ticker.C:
					if err := addEntryToIndex(tick.Second()); err != nil {
						return err
					}
				}
			}
		}
	}

	return func() error {
		startedWg.Done()

		for i := 0; ; i++ {
			select {
			case <-ctx.Done():
				return nil
			default:
				if err := addEntryToIndex(i); err != nil {
					return err
				}
			}
		}
	}
}

func (t *TestRun) queryRoutine(ctx context.Context, startedWg *sync.WaitGroup) func() error {
	return func() error {
		ticker := time.NewTicker(time.Duration(int64(float64(1000000000) / float64(t.queriesPerSec))))
		startedWg.Done()
		for {
			select {
			case <-ctx.Done():
				return nil
			case <-ticker.C:
				go t.runQuery()
			}
		}
	}
}

func (t *TestRun) runQuery() {
	pattern := t.queryPatternGenerator.GetPattern()
	pre := time.Now()

	queryStartedID := t.stats.incQueriesStarted()
	if queryStartedID > t.totalQueryCount {
		return
	}

	_, err := t.index.Find(orgID, pattern, 0)
	if err != nil {
		log.Printf("Warning: Query failed with error: %s", err)
	}

	queryCompletedID := t.stats.incQueriesCompleted()
	t.stats.addQueryTime(queryCompletedID, time.Now().Sub(pre))
	if queryCompletedID%t.querySampleFactor == 0 {
		log.Printf("Sample: queried for pattern %s", pattern)
	}
}
