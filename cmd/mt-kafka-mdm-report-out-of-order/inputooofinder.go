package main

import (
	"os"
	"sync"
	"time"

	"github.com/grafana/metrictank/idx/cassandra"
	"github.com/grafana/metrictank/mdata"
	"github.com/grafana/metrictank/mdata/errors"
	"github.com/grafana/metrictank/schema"
	"github.com/grafana/metrictank/schema/msg"
	log "github.com/sirupsen/logrus"
)

type Track struct {
	Name string
	Tags []string

	reorderBuffer *mdata.ReorderBuffer

	Count           int
	OutOfOrderCount int
	DuplicateCount  int
}

type Tracker map[schema.MKey]Track

// find out-of-order and duplicate metrics
type inputOOOFinder struct {
	reorderWindow uint32
	tracker       Tracker

	lock sync.Mutex
}

func (ip inputOOOFinder) Tracker() Tracker {
	return ip.tracker
}

func newInputOOOFinder(partitionFrom int, partitionTo int, reorderWindow uint32) *inputOOOFinder {
	cassandraIndex := cassandra.New(cassandra.CliConfig)
	err := cassandraIndex.InitBare()
	if err != nil {
		log.Fatalf("error initializing cassandra index: %s", err.Error())
		os.Exit(1)
	}

	metricDefinitions := make([]schema.MetricDefinition, 0)
	for partition := partitionFrom; (partitionTo == -1 && partition == partitionFrom) || (partitionTo > 0 && partition < partitionTo); partition++ {
		metricDefinitions = cassandraIndex.LoadPartitions([]int32{int32(partition)}, metricDefinitions, time.Now())
	}

	tracker := Tracker{}
	for _, metricDefinition := range metricDefinitions {
		tracker[metricDefinition.Id] = Track{
			Name:            metricDefinition.Name,
			Tags:            metricDefinition.Tags,
			reorderBuffer:   mdata.NewReorderBuffer(reorderWindow, uint32(metricDefinition.Interval), false),
			Count:           0,
			OutOfOrderCount: 0,
			DuplicateCount:  0,
		}
	}

	return &inputOOOFinder{
		reorderWindow: reorderWindow,
		tracker:       tracker,

		lock: sync.Mutex{},
	}
}

func (ip *inputOOOFinder) incrementCounts(metricKey schema.MKey, metricTime int64, track Track, partition int32) {
	track.Count++

	_, err := track.reorderBuffer.Add(uint32(metricTime), 0) // ignore value
	if err == errors.ErrMetricTooOld {
		track.OutOfOrderCount++
	} else if err == errors.ErrMetricNewValueForTimestamp {
		track.DuplicateCount++
	} else if err != nil {
		log.Errorf("failed to add metric with Name=%q and timestamp=%d from partition=%d to reorder buffer: %s", track.Name, metricTime, partition, err)
		return
	}

	ip.tracker[metricKey] = track
}

func (ip *inputOOOFinder) ProcessMetricData(metric *schema.MetricData, partition int32) {
	metricKey, err := schema.MKeyFromString(metric.Id)
	if err != nil {
		log.Errorf("failed to get metric key from id=%v: %s", metric.Id, err.Error())
		return
	}

	ip.lock.Lock()
	defer ip.lock.Unlock()

	track, exists := ip.tracker[metricKey]
	if !exists {
		ip.tracker[metricKey] = Track{
			Name:            metric.Name,
			Tags:            metric.Tags,
			reorderBuffer:   mdata.NewReorderBuffer(ip.reorderWindow, uint32(metric.Interval), false),
			Count:           0,
			OutOfOrderCount: 0,
			DuplicateCount:  0,
		}
	}

	ip.incrementCounts(metricKey, metric.Time, track, partition)
}

func (ip *inputOOOFinder) ProcessMetricPoint(mp schema.MetricPoint, format msg.Format, partition int32) {
	ip.lock.Lock()
	defer ip.lock.Unlock()

	track, exists := ip.tracker[mp.MKey]
	if !exists {
		log.Errorf("track for metric with key=%v from partition=%d not found", mp.MKey, partition)
		return
	}

	ip.incrementCounts(mp.MKey, int64(mp.Time), track, partition)
}

func (ip *inputOOOFinder) ProcessIndexControlMsg(msg schema.ControlMsg, partition int32) {

}
