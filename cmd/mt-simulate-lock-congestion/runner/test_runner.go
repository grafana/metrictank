package runner

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/grafana/metrictank/idx"
	"github.com/grafana/metrictank/idx/memory"
	"github.com/raintank/schema"
	"golang.org/x/sync/errgroup"
)

// TestRun represents one test run, including all the structures used for the test
type TestRun struct {
	index                 idx.MetricIndex
	metricNameGenerator   metricNameGenerator
	queryPatternGenerator queryPatternGenerator
	stats                 runStats
	addsPerSec            uint32 // how many adds per second we want to execute. 0 means unlimited, as many as possible
	addThreads            uint32 // number of concurrent add threads
	queriesPerSec         uint32 // how many queries per second we want to execute. 0 means unlimited, as many as possible
	queryThreads          uint32 // number of concurrent query threads
	runDuration           time.Duration
}

const orgID = 1

// NewTestRun Instantiates a new test run
func NewTestRun(addsPerSec, addThreads, queriesPerSec, queryThreads uint32, runDuration time.Duration) *TestRun {
	runner := TestRun{
		stats:                 runStats{queryTimes: make([]uint32, queriesPerSec*uint32(runDuration.Seconds()))},
		index:                 memory.New(),
		metricNameGenerator:   newMetricNameGenerator(),
		queryPatternGenerator: newQueryPatternGenerator(),
		addsPerSec:            addsPerSec,
		addThreads:            addThreads,
		queriesPerSec:         queriesPerSec,
		queryThreads:          queryThreads,
		runDuration:           runDuration,
	}

	return &runner
}

// Run executes the run
func (t *TestRun) Run() {
	mainCtx, cancel := context.WithCancel(context.Background())

	// wait group to wait until all routines have been started
	startedWg := sync.WaitGroup{}

	addThreads, addCtx := errgroup.WithContext(mainCtx)
	startedWg.Add(int(t.addThreads))
	for i := uint32(0); i < t.addThreads; i++ {
		addThreads.Go(t.addRoutine(addCtx, &startedWg, i))
	}

	queryThreads, queryCtx := errgroup.WithContext(mainCtx)
	queryRoutine := t.queryRoutine(queryCtx, &startedWg)
	startedWg.Add(int(t.queryThreads))
	for i := uint32(0); i < t.queryThreads; i++ {
		queryThreads.Go(queryRoutine)
	}

	startedWg.Wait()

	// as soon as all routines have started, we start the timer
	<-time.After(t.runDuration)
	cancel()
}

// PrintStats writes all the statistics in human readable format into stdout
func (t *TestRun) PrintStats() {
	t.stats.Print(uint32(t.runDuration.Seconds()))
}

func (t *TestRun) addRoutine(ctx context.Context, startedWg *sync.WaitGroup, routineID uint32) func() error {
	partitionID := int32(routineID)

	addEntryToIndex := func(time int) error {
		md := schema.MetricData{OrgId: orgID, Interval: 1, Value: 2, Time: int64(time), Mtype: "gauge"}
		md.Name = t.metricNameGenerator.getNewMetricName()
		md.SetId()
		key, err := schema.MKeyFromString(md.Id)
		if err != nil {
			return fmt.Errorf("Unexpected error when generating MKey from ID string: %s", err)
		}

		t.index.AddOrUpdate(key, &md, partitionID)
		t.stats.incAddsCompleted()
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
	queryIndex := func() error {
		pattern := t.queryPatternGenerator.getPattern(t.metricNameGenerator.getExistingMetricName())
		pre := time.Now()
		_, err := t.index.Find(orgID, pattern, 0)
		t.stats.addQueryTime(uint32(time.Now().Sub(pre).Nanoseconds() / int64(1000000)))
		t.stats.incQueriesCompleted()
		return err
	}

	if t.queriesPerSec > 0 {
		interval := time.Duration(int64(1000000000) * int64(t.queryThreads) / int64(t.queriesPerSec))

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
				case <-ticker.C:
					if err := queryIndex(); err != nil {
						return err
					}
				}
			}
		}
	}

	return func() error {
		for {
			select {
			case <-ctx.Done():
				return nil
			default:
				if err := queryIndex(); err != nil {
					return err
				}
			}
		}
	}
}
