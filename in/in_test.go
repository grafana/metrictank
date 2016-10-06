package in

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/raintank/met"
	"github.com/raintank/met/helper"
	"github.com/raintank/metrictank/cluster"
	"github.com/raintank/metrictank/idx"
	"github.com/raintank/metrictank/idx/memory"
	"github.com/raintank/metrictank/mdata"
	"github.com/raintank/metrictank/mdata/chunk"

	"gopkg.in/raintank/schema.v1"
)

// handler.HandleMessage some messages concurrently and make sure the entries in defcache are correct
// this can expose bad reuse of data arrays in the handler and such
func Test_HandleMessage(t *testing.T) {
	stats, _ := helper.New(false, "", "standard", "metrictank", "")
	cluster.InitManager("default", "test", false, time.Now(), &cluster.Murmur2Partitioner{})
	mdata.InitMetrics(stats)

	for i := 0; i < 100; i++ {
		test_HandleMessage(t, stats)
	}
}

func test_HandleMessage(t *testing.T, stats met.Backend) {

	store := mdata.NewDevnullStore()
	aggmetrics := mdata.NewAggMetrics(store, 600, 10, 800, 8000, 10000, 0, make([]mdata.AggSetting, 0))
	metricIndex := memory.New()
	metricIndex.Init(stats)

	// mimic how we use nsq handlers:
	// handlers operate concurrently, but within 1 handler, the handling is sequential

	consumer := func(in chan []byte, group *sync.WaitGroup, aggmetrics mdata.Metrics, metricIndex idx.MetricIndex) {
		handler := New(aggmetrics, metricIndex, nil, "test", stats)
		for msg := range in {
			handler.Handle(msg)
		}
		group.Done()
	}

	handlePool := make(chan []byte, 100)
	handlerGroup := sync.WaitGroup{}
	handlerGroup.Add(5)
	for i := 0; i != 5; i++ {
		go consumer(handlePool, &handlerGroup, aggmetrics, metricIndex)
	}

	// timestamps start at 1 and go up from there. (we can't use 0, see AggMetric.Add())
	// handle 5 messages, each containing different metrics

	// i m id
	// 0 0 1
	// 0 1 2
	// 0 2 3
	// 0 3 4

	// 1 0 2*
	// 1 1 4*
	// 1 2 6
	// 1 3 8

	// 2 0 3*
	// 2 1 6*
	// 2 2 9
	// 2 3 12

	// 3 0 4*
	// 3 1 8*
	// 3 2 12*
	// 3 3 16

	// -> 9 unique id's == 9 unique metrics cause metrics only differ due to the id
	wg := &sync.WaitGroup{}
	wg.Add(4)
	type idToId struct {
		sync.Mutex
		ids map[string]int
	}
	tit := &idToId{
		ids: make(map[string]int),
	}
	for i := 0; i < 4; i++ {
		go func(i int, tit *idToId, handlePool chan []byte) {
			var metric *schema.MetricData
			for m := 0; m < 4; m++ {
				id := (i + 1) * (m + 1)
				t.Logf("worker %d metric %d -> adding metric with id and orgid %d", i, m, id)

				metric = &schema.MetricData{
					Id:       "",
					OrgId:    id,
					Name:     fmt.Sprintf("some.id.%d", id),
					Metric:   fmt.Sprintf("some.id.%d", id),
					Interval: 60,
					Value:    1234.567,
					Unit:     "ms",
					Time:     int64(id),
					Mtype:    "gauge",
					Tags:     []string{fmt.Sprintf("%d", id)},
				}
				metric.SetId()
				tit.Lock()
				tit.ids[metric.Id] = id
				tit.Unlock()
				var data []byte
				var err error
				data, err = metric.MarshalMsg(data[:])
				if err != nil {
					t.Fatal(err.Error())
				}
				handlePool <- data
			}
			wg.Done()
		}(i, tit, handlePool)
	}
	wg.Wait()
	close(handlePool)
	handlerGroup.Wait()
	defs := metricIndex.List(-1)
	if len(defs) != 9 {
		t.Fatalf("query for org -1 should result in 9 distinct metrics. not %d", len(defs))
	}

	for _, d := range defs {
		id := tit.ids[d.Id]
		if d.Name != fmt.Sprintf("some.id.%d", id) {
			t.Fatalf("incorrect name for %s : %s", d.Id, d.Name)
		}
		if d.OrgId != id {
			t.Fatalf("incorrect OrgId for %s : %d", d.Id, d.OrgId)
		}
		if d.Tags[0] != fmt.Sprintf("%d", id) {
			t.Fatalf("incorrect tags for %s : %s", d.Id, d.Tags)
		}
	}

	defs = metricIndex.List(2)
	if len(defs) != 1 {
		t.Fatalf("len of defs should be exactly 1. got defs with len %d: %v", len(defs), defs)
	}
	d := defs[0]
	if d.OrgId != 2 {
		t.Fatalf("incorrect metricdef returned: %v", d)
	}
}

func BenchmarkHandler_HandleMessage(b *testing.B) {
	stats, _ := helper.New(false, "", "standard", "metrictank", "")
	cluster.InitManager("default", "test", false, time.Now(), &cluster.Murmur2Partitioner{})
	mdata.InitMetrics(stats)
	store := mdata.NewDevnullStore()
	aggmetrics := mdata.NewAggMetrics(store, 600, 10, 800, 8000, 10000, 0, make([]mdata.AggSetting, 0))
	metricIndex := memory.New()
	metricIndex.Init(stats)
	handler := New(aggmetrics, metricIndex, nil, "test", stats)

	msgs := make([][]byte, b.N)
	var metric *schema.MetricData
	for i := 0; i < b.N; i++ {
		metric = &schema.MetricData{
			Id:       fmt.Sprintf("some.id.of.a.metric.%d", i),
			OrgId:    500,
			Name:     fmt.Sprintf("some.id.%d", i),
			Metric:   "metric",
			Interval: 60,
			Value:    1234.567,
			Unit:     "ms",
			Time:     int64(1),
			Mtype:    "gauge",
			Tags:     []string{"some_tag", "ok", fmt.Sprintf("id%d", i)},
		}
		metric.SetId()
		var data []byte
		var err error
		data, err = metric.MarshalMsg(data[:])
		if err != nil {
			b.Fatal(err.Error())
		}
		msgs[i] = data
	}
	finished := make(chan struct{})
	go func() {
		for {
			select {
			case <-chunk.TotalPoints:
			case <-finished:
				return
			}
		}
	}()
	b.ResetTimer()
	for _, msg := range msgs {
		handler.Handle(msg)
	}
	metricIndex.Stop()
	cluster.ThisCluster.Stop()
	store.Stop()
	close(finished)
}
