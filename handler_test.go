package main

import (
	"fmt"
	"sync"
	"testing"

	"github.com/nsqio/go-nsq"
	"github.com/raintank/met"
	"github.com/raintank/met/helper"
	"github.com/raintank/metrictank/defcache"
	Nsq "github.com/raintank/metrictank/in/nsq"
	"github.com/raintank/metrictank/mdata"
	"github.com/raintank/metrictank/metricdef"
	"gopkg.in/raintank/schema.v0"
	"gopkg.in/raintank/schema.v0/msg"
)

// handler.HandleMessage some messages concurrently and make sure the entries in defcache are correct
// this can expose bad reuse of data arrays in the handler and such
func Test_HandleMessage(t *testing.T) {

	stats, _ := helper.New(false, "", "standard", "metrictank", "")
	mdata.CluStatus = mdata.NewClusterStatus("default", false)
	initMetrics(stats)
	mdata.InitMetrics(stats)

	for i := 0; i < 100; i++ {
		test_HandleMessage(t, stats)
	}
}

func test_HandleMessage(t *testing.T, stats met.Backend) {

	store := mdata.NewDevnullStore()
	aggmetrics := mdata.NewAggMetrics(store, 600, 10, 800, 8000, 10000, 0, make([]mdata.AggSetting, 0))
	defCache := defcache.New(metricdef.NewDefsMockConcurrent(), stats)

	// mimic how we use nsq handlers:
	// handlers operate concurrently, but within 1 handler, the handling is sequential

	consumer := func(in chan *nsq.Message, group *sync.WaitGroup, aggmetrics mdata.Metrics, defCache *defcache.DefCache) {
		handler := Nsq.NewHandler(aggmetrics, defCache, nil, stats)
		for msg := range in {
			err := handler.HandleMessage(msg)
			if err != nil {
				panic(err)
			}
		}
		group.Done()
	}

	handlePool := make(chan *nsq.Message, 100)
	handlerGroup := sync.WaitGroup{}
	handlerGroup.Add(5)
	for i := 0; i != 5; i++ {
		go consumer(handlePool, &handlerGroup, aggmetrics, defCache)
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
		go func(i int, tit *idToId, handlePool chan *nsq.Message) {
			metrics := make([]*schema.MetricData, 4)
			for m := 0; m < len(metrics); m++ {
				id := (i + 1) * (m + 1)
				//t.Logf("worker %d metric %d -> adding metric with id and orgid %d", i, m, id)

				metrics[m] = &schema.MetricData{
					Id:         "",
					OrgId:      id,
					Name:       fmt.Sprintf("some.id.%d", id),
					Metric:     "metric",
					Interval:   60,
					Value:      1234.567,
					Unit:       "ms",
					Time:       int64(id),
					TargetType: "gauge",
					Tags:       []string{fmt.Sprintf("%d", id)},
				}
				metrics[m].SetId()
				tit.Lock()
				tit.ids[metrics[m].Id] = id
				tit.Unlock()
			}
			id := nsq.MessageID{'1', '2', '3', '4', '5', '6', '7', '8', '9', '0', 'a', 's', 'd', 'f', 'g', 'h'}
			data, err := msg.CreateMsg(metrics, 1, msg.FormatMetricDataArrayMsgp)
			if err != nil {
				panic(err)
			}
			msg := nsq.NewMessage(id, data)
			handlePool <- msg
			wg.Done()
		}(i, tit, handlePool)
	}
	wg.Wait()
	close(handlePool)
	handlerGroup.Wait()
	defs := defCache.List(-1)
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
	defs = defCache.List(2)
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
	mdata.CluStatus = mdata.NewClusterStatus("default", false)
	initMetrics(stats)

	store := mdata.NewDevnullStore()
	aggmetrics := mdata.NewAggMetrics(store, 600, 10, 800, 8000, 10000, 0, make([]mdata.AggSetting, 0))
	defCache := defcache.New(metricdef.NewDefsMock(), stats)
	handler := Nsq.NewHandler(aggmetrics, defCache, nil, stats)

	metrics := make([]*schema.MetricData, 10)
	for i := 0; i < len(metrics); i++ {
		metrics[i] = &schema.MetricData{
			Id:         "some.id.of.a.metric",
			OrgId:      500,
			Name:       "some.id",
			Metric:     "metric",
			Interval:   60,
			Value:      1234.567,
			Unit:       "ms",
			Time:       int64(i - len(metrics) + 1),
			TargetType: "gauge",
			Tags:       []string{"some_tag", "ok"},
		}
	}
	// timestamps start at 1 and go up from there. (we can't use 0, see AggMetric.Add())
	msgs := make([]*nsq.Message, b.N)
	for i := 0; i < len(msgs); i++ {
		id := nsq.MessageID{'1', '2', '3', '4', '5', '6', '7', '8', '9', '0', 'a', 's', 'd', 'f', 'g', 'h'}
		for j := 0; j < len(metrics); j++ {
			metrics[j].Time += 10
		}
		data, err := msg.CreateMsg(metrics, 1, msg.FormatMetricDataArrayMsgp)
		if err != nil {
			panic(err)
		}
		msgs[i] = nsq.NewMessage(id, data)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := handler.HandleMessage(msgs[i])
		if err != nil {
			panic(err)
		}
	}
}
