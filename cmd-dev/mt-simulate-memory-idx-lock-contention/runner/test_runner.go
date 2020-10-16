package runner

import (
	"context"
	"fmt"
	"hash/fnv"
	"sync"
	"time"

	"github.com/grafana/metrictank/conf"
	"github.com/grafana/metrictank/idx"
	"github.com/grafana/metrictank/idx/memory"
	"github.com/grafana/metrictank/mdata"
	"github.com/grafana/metrictank/schema"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
)

// TestRun represents one test run, including all the structures used for the test
type TestRun struct {
	index            idx.MetricIndex
	metricsChan      chan *schema.MetricData
	queriesChan      chan string
	addsPerSec       uint32 // how many adds per second we want to execute. 0 means unlimited, as many as possible
	addThreads       uint32 // number of concurrent add threads
	initialIndexSize uint32 // prepopulate the index with the defined number of entries before starting the actual test run
	queriesPerSec    uint32 // how many queries per second we want to execute. 0 means unlimited, as many as possible
	concQueries      int    // how many queries we want to execute concurrently.
	startTime        time.Time

	workers *errgroup.Group
	ctx     context.Context

	done chan struct{}
}

var (
	metricAdd    = NewStat("metric-add")
	metricUpdate = NewStat("metric-update")
	queryExec    = NewStat("query")
)

const orgID = 1

// NewTestRun Instantiates a new test run
func NewTestRun(metricsChan chan *schema.MetricData, queriesChan chan string, addsPerSec, addThreads, initialIndexSize, queriesPerSec uint32, concQueries int) *TestRun {
	index := memory.New()
	index.Init()
	// initializing with a `nil` store, that's a bit risky but good enough for the moment
	mdata.Schemas = conf.NewSchemas(nil)

	runner := TestRun{
		index:            index,
		metricsChan:      metricsChan,
		queriesChan:      queriesChan,
		addsPerSec:       addsPerSec,
		addThreads:       addThreads,
		initialIndexSize: initialIndexSize,
		queriesPerSec:    queriesPerSec,
		concQueries:      concQueries,
		done:             make(chan struct{}),
	}

	return &runner
}

func (t *TestRun) Wait() {
	<-t.done
}

// Init prepares to run
func (t *TestRun) Init(ctx context.Context) {
	t.workers, t.ctx = errgroup.WithContext(ctx)
	log.Printf("TestRun.Init: index pre-population starting")
	t.prepopulateIndex()
	log.Printf("TestRun.Init: pre-populated the index with %d entries", t.initialIndexSize)
	t.startTime = time.Now()
	log.Printf("TestRun.Init: launching query routine")
	t.workers.Go(t.queryRoutine(t.ctx))
	log.Printf("TestRun.Init: done")
}

// Run executes the run
func (t *TestRun) Run() {
	log.Printf("TestRun.Run: starting index add routines")
	mdChans := make([]chan *schema.MetricData, t.addThreads)
	for i := uint32(0); i < t.addThreads; i++ {
		ch := make(chan *schema.MetricData, 1000)
		mdChans[i] = ch
		partition := int32(i)
		t.workers.Go(t.addRoutine(t.ctx, ch, partition))
	}
	log.Printf("TestRun.Run: starting index routing routine")
	t.workers.Go(t.routeMetrics(t.ctx, mdChans))
	log.Printf("TestRun.Run: starting benchmark")

	t.workers.Wait()
	log.Printf("TestRun.Run: benchmark complete")
	t.PrintStats()
	close(t.done)
}

// PrintStats writes all the statistics in human readable format into stdout
func (t *TestRun) PrintStats() {
	metricAdd.Report()
	fmt.Println()
	metricUpdate.Report()
	fmt.Println()
	queryExec.Report()
	fmt.Println()
	fmt.Println()
	metricAdd.ReportComputer()
	metricUpdate.ReportComputer()
	queryExec.ReportComputer()
}

func getPartitionFromName(name string, partitionCount uint32) int32 {
	h := fnv.New32a()
	h.Write([]byte(name))
	p := int32(h.Sum32() % partitionCount)
	if p < 0 {
		p = p * -1
	}
	return p
}

func (t *TestRun) prepopulateIndex() {
	for i := uint32(0); i < t.initialIndexSize; i++ {
		md := <-t.metricsChan
		partitionID := getPartitionFromName(md.Name, t.addThreads)
		key, _ := schema.MKeyFromString(md.Id)
		t.index.AddOrUpdate(key, md, partitionID)
	}
}

func (t *TestRun) routeMetrics(ctx context.Context, mdChans []chan *schema.MetricData) func() error {
	return func() error {
		log.Printf("routeMetrics thread started")
		defer log.Printf("routeMetrics thread ended")
		limiter := rate.NewLimiter(rate.Limit(t.addsPerSec), int(t.addsPerSec))
		for {
			select {
			case <-ctx.Done():
				log.Printf("routeMetrics thread shutting down")
				for _, ch := range mdChans {
					close(ch)
				}
				return nil
			case md := <-t.metricsChan:
				if md == nil {
					log.Printf("routeMetrics thread shutting down")
					for _, ch := range mdChans {
						close(ch)
					}
					return nil
				}
				limiter.Wait(ctx)
				partitionID := getPartitionFromName(md.Name, t.addThreads)
				ch := mdChans[partitionID]
				ch <- md
			}
		}
	}
}

func (t *TestRun) addRoutine(ctx context.Context, in chan *schema.MetricData, partitionID int32) func() error {
	return func() error {
		log.Printf("addRoutine(%d) thread started", partitionID)
		defer log.Printf("addRoutine(%d) thread ended", partitionID)
		for md := range in {
			key, _ := schema.MKeyFromString(md.Id)
			pre := time.Now()
			_, _, update := t.index.AddOrUpdate(key, md, partitionID)
			if !update {
				metricAdd.Add(time.Since(pre))
			} else {
				metricUpdate.Add(time.Since(pre))
			}
		}
		return nil
	}

}

func (t *TestRun) queryRoutine(ctx context.Context) func() error {
	return func() error {
		log.Printf("queryRoutine thread started")
		defer log.Printf("queryRoutine thread ended")
		limiter := rate.NewLimiter(rate.Limit(t.queriesPerSec), int(t.queriesPerSec))
		var wg sync.WaitGroup
		active := make(chan struct{}, t.concQueries)
		count := 0
		ticker := time.NewTicker(time.Second * 5)
	LOOP:
		for {
			select {
			case <-ctx.Done():
				break LOOP
			case <-ticker.C:
				log.Printf("%d find queries active. %d launched", len(active), count)
			case pattern, ok := <-t.queriesChan:
				if !ok {
					break LOOP
				}
				limiter.Wait(ctx)
				wg.Add(1)
				count++
				go t.runQuery(pattern, &wg, active)
			}
		}
		log.Printf("queryRoutine shutting down. Waiting for %d running finds to complete", len(active))
		wg.Wait()
		return nil
	}
}

func (t *TestRun) runQuery(pattern string, wg *sync.WaitGroup, active chan struct{}) {
	defer func() {
		<-active
		wg.Done()
	}()
	pre := time.Now()
	active <- struct{}{}
	_, err := t.index.Find(orgID, pattern, 0, 0)
	if err != nil {
		log.Printf("Warning: Query failed with error: %s", err)
	}

	queryExec.Add(time.Since(pre))
}
