package runner

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/grafana/metrictank/cmd/mt-simulate-lock-congestion/metricname"
	"github.com/grafana/metrictank/cmd/mt-simulate-lock-congestion/query"
	"github.com/grafana/metrictank/conf"
	"github.com/grafana/metrictank/input"
	"github.com/grafana/metrictank/mdata"

	"github.com/grafana/metrictank/idx"
	"github.com/grafana/metrictank/idx/memory"
	"github.com/raintank/schema"
	"github.com/raintank/schema/msg"
	"golang.org/x/sync/errgroup"
)

type addedMetricData struct {
	md        *schema.MetricData
	partition int32
}

// TestRun represents one test run, including all the structures used for the test
type TestRun struct {
	index                 idx.MetricIndex
	handler               input.Handler
	metricNameGenerator   metricname.NameGenerator
	queryPatternGenerator query.QueryGenerator
	addedMdChan           chan addedMetricData
	addedMdLock           sync.RWMutex
	addedMds              []addedMetricData
	stats                 runStats
	addsPerSec            uint32 // how many adds per second we want to execute. 0 means unlimited, as many as possible
	addThreads            uint32 // number of concurrent add threads
	addSampleFactor       uint32 // how often to print info about a completed add
	addDelay              uint32 // once the benchmark starts the querying threads start, if addDelay is >0 then the adding into the index is delayed by the given number of seconds
	updatesPerAdd         uint32 // how many metric updates should be done between each metric add, they also get called by the add threads
	initialIndexSize      uint32 // prepopulate the index with the defined number of entries before starting the actual test run
	queriesPerSec         uint32 // how many queries per second we want to execute. 0 means unlimited, as many as possible
	querySampleFactor     uint32 // how often to print info about a completed query
	totalQueryCount       uint32 // the total number of queries we will start
	runDuration           time.Duration
}

const orgID = 1

// NewTestRun Instantiates a new test run
func NewTestRun(nameGenerator metricname.NameGenerator, queryGenerator query.QueryGenerator, addDelay, addsPerSec, addThreads, addSampleFactor, updatesPerAdd, initialIndexSize, queriesPerSec, querySampleFactor uint32, runDuration time.Duration) *TestRun {
	totalQueryCount := (queriesPerSec + addDelay) * uint32(runDuration.Seconds())

	index := memory.New()
	// initializing with a `nil` store, that's a bit risky but good enough for the moment
	mdata.Schemas = conf.NewSchemas(nil)
	metrics := mdata.NewAggMetrics(nil, nil, false, 3600, 7200, 3600)
	handler := input.NewDefaultHandler(metrics, index, "lock-congestion-simulator")

	runner := TestRun{
		index:                 index,
		handler:               handler,
		stats:                 newRunStats(totalQueryCount),
		totalQueryCount:       totalQueryCount,
		metricNameGenerator:   nameGenerator,
		queryPatternGenerator: queryGenerator,
		addedMdChan:           make(chan addedMetricData, 100),
		addsPerSec:            addsPerSec,
		addThreads:            addThreads,
		addSampleFactor:       addSampleFactor,
		addDelay:              addDelay,
		updatesPerAdd:         updatesPerAdd,
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

	// wait group to wait until all routines have been started
	startedWg := sync.WaitGroup{}
	workerThreads, workerCtx := errgroup.WithContext(mainCtx)

	startedWg.Add(1)
	workerThreads.Go(t.addedMdTracker(workerCtx, &startedWg))

	log.Printf("Starting the metric name generator")
	t.metricNameGenerator.Start(mainCtx, t.addThreads)

	log.Printf("Starting the query generator")
	t.queryPatternGenerator.Start()

	log.Printf("Prepopulating the index with %d entries", t.initialIndexSize)
	t.prepopulateIndex()

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

func (t *TestRun) addedMdTracker(ctx context.Context, startedWg *sync.WaitGroup) func() error {
	buffer := make([]addedMetricData, 0, 1000)
	trackUpTo := 1000000 // how many added MDs to track at max
	return func() error {
		startedWg.Done()
		for {
			select {
			case md := <-t.addedMdChan:
				if len(buffer) < cap(buffer) {
					buffer = append(buffer, md)
				} else {
					t.addedMdLock.Lock()
					if len(t.addedMds) < trackUpTo {
						for i := range buffer {
							t.addedMds = append(t.addedMds, buffer[i])
						}
					} else {
						startingAt := rand.Intn(trackUpTo)
						for i := 0; i < len(buffer); i++ {
							t.addedMds[(startingAt+i)%trackUpTo] = buffer[i]
						}
					}
					t.addedMdLock.Unlock()
					buffer = buffer[:0]
				}
			case <-ctx.Done():
				fmt.Println("addedMdTracker is shutting down")
				return nil
			}
		}
	}
}

func (t *TestRun) getAddedMds(count int) []addedMetricData {
	res := make([]addedMetricData, count)
	t.addedMdLock.RLock()
	defer t.addedMdLock.RUnlock()

	addedMdsLen := len(t.addedMds)
	if count > addedMdsLen || addedMdsLen == 0 {
		return nil
	}

	startingAt := rand.Intn(addedMdsLen)
	for i := 0; i < count; i++ {
		res[i] = t.addedMds[(startingAt+i)%addedMdsLen]
	}

	return res
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

		partitionID := int32(i % t.addThreads)
		t.index.AddOrUpdate(key, &md, partitionID)
		t.addedMdChan <- addedMetricData{&md, partitionID}
	}
}

func (t *TestRun) addRoutine(ctx context.Context, startedWg *sync.WaitGroup, routineID uint32) func() error {
	partitionID := int32(routineID)

	addEntryToIndex := func(time int) error {
		md := schema.MetricData{OrgId: orgID, Interval: 1, Value: 2, Time: int64(time), Mtype: "gauge"}
		md.Name = t.metricNameGenerator.GetNewMetricName()
		md.SetId()
		t.handler.ProcessMetricData(&md, partitionID)
		adds := t.stats.incAddsCompleted()
		if adds%t.addSampleFactor == 0 {
			log.Printf("Sample: added metric name to index %s", md.Name)
		}

		t.addedMdChan <- addedMetricData{&md, partitionID}

		addedMds := t.getAddedMds(int(t.updatesPerAdd))
		for i := range addedMds {
			mkey, err := schema.MKeyFromString(addedMds[i].md.Id)
			if err != nil {
				log.Fatalf("Unexpected error when getting mkey from string \"%s\": %s", addedMds[i].md.Id, err.Error())
			}
			t.handler.ProcessMetricPoint(schema.MetricPoint{
				MKey:  mkey,
				Value: addedMds[i].md.Value,
				Time:  uint32(addedMds[i].md.Time),
			},
				msg.FormatMetricPoint,
				addedMds[i].partition,
			)
		}

		t.stats.incUpdatesCompleted(t.updatesPerAdd)

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
				if err := addEntryToIndex(int(time.Now().Unix())); err != nil {
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

	res, err := t.index.Find(orgID, pattern, 0)
	if err != nil {
		log.Printf("Warning: Query failed with error: %s", err)
	}

	queryCompletedID := t.stats.incQueriesCompleted()
	t.stats.addQueryTime(queryCompletedID, time.Now().Sub(pre))
	if queryCompletedID%t.querySampleFactor == 0 {
		log.Printf("Sample: query got %d results for pattern %s", len(res), pattern)
	}
}
