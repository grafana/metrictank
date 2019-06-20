package runner

import (
	"context"
	"fmt"
	"sync/atomic"
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
	addsPerSec            uint32 // how many adds per second we want to execute. 0 means unlimited, as many as possible
	addsSkipped           uint32 // counter that keeps track how many adds got skipped because the index wasn't fast enough
	addThreads            uint32 // number of concurrent add threads
	queriesPerSec         uint32 // how many queries per second we want to execute. 0 means unlimited, as many as possible
	queriesSkipped        uint32 // counter that keeps track of how many queries got skipped because the index wasn't fast enough
	queryThreads          uint32 // number of concurrent query threads
	runDuration           time.Duration
}

const orgID = 1

// NewTestRun Instantiates a new test run
func NewTestRun(addsPerSec, addThreads, queriesPerSec, queryThreads uint32, runDuration time.Duration) *TestRun {
	runner := TestRun{
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
	addThreads, addCtx := errgroup.WithContext(mainCtx)
	for i := uint32(0); i < t.addThreads; i++ {
		addThreads.Go(t.addRoutine(addCtx, i))
	}

	queryThreads, queryCtx := errgroup.WithContext(mainCtx)
	for i := uint32(0); i < t.queryThreads; i++ {
		queryThreads.Go(t.queryRoutine(queryCtx))
	}

	<-time.After(t.runDuration)
	cancel()
}

func (t *TestRun) addRoutine(ctx context.Context, routineID uint32) func() error {
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
		return nil
	}

	if t.addsPerSec > 0 {
		interval := time.Second / time.Duration(t.addsPerSec) / time.Duration(t.addThreads)

		return func() error {
			ticker := time.NewTicker(interval)
			go func() {
				<-ctx.Done()
				ticker.Stop()
			}()

			var previousTick time.Time

			for {
				select {
				case <-ctx.Done():
					return nil
				case tick := <-ticker.C:
					if !previousTick.IsZero() {
						if previousTick.Add(interval).Before(tick) {
							atomic.AddUint32(&t.addsSkipped, uint32(tick.Sub(previousTick)/interval))
						}
					}
					previousTick = tick

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

func (t *TestRun) queryRoutine(ctx context.Context) func() error {
	queryIndex := func() error {
		pattern := t.queryPatternGenerator.getPattern(t.metricNameGenerator.getExistingMetricName())
		_, err := t.index.Find(orgID, pattern, 0)
		return err
	}

	if t.queriesPerSec > 0 {
		interval := time.Second / time.Duration(t.queriesPerSec) / time.Duration(t.queryThreads)

		return func() error {
			ticker := time.NewTicker(interval)
			go func() {
				<-ctx.Done()
				ticker.Stop()
			}()

			var previousTick time.Time

			for {
				select {
				case <-ctx.Done():
					return nil
				case tick := <-ticker.C:
					if !previousTick.IsZero() {
						if previousTick.Add(interval).Before(tick) {
							atomic.AddUint32(&t.addsSkipped, uint32(tick.Sub(previousTick)/interval))
						}
					}
					previousTick = tick

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
