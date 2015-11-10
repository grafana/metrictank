package main

import (
	"bytes"
	"encoding/gob"
	"os"
	"sync"
	"time"

	"github.com/grafana/grafana/pkg/log"
	gometrics "github.com/rcrowley/go-metrics"
)

var points = gometrics.NewHistogram(gometrics.NewExpDecaySample(1028, 0.015))

type AggMetrics struct {
	sync.Mutex
	Metrics      map[string]*AggMetric
	chunkSpan    uint32
	numChunks    uint32
	aggSpan      uint32
	aggChunkSpan uint32
	aggNumChunks uint32
}

func NewAggMetrics(chunkSpan, numChunks, aggSpan, aggChunkSpan, aggNumChunks uint32) *AggMetrics {
	ms := AggMetrics{
		Metrics:      make(map[string]*AggMetric),
		chunkSpan:    chunkSpan,
		numChunks:    numChunks,
		aggSpan:      aggSpan,
		aggChunkSpan: aggChunkSpan,
		aggNumChunks: aggNumChunks,
	}
	// open data file
	dataFile, err := os.Open(*dumpFile)

	if err == nil {
		log.Info("loading aggMetrics from file.")
		dataDecoder := gob.NewDecoder(dataFile)
		err = dataDecoder.Decode(&ms)
		if err != nil {
			log.Error(1, "failed to load aggMetrics from file. %s", err)
		}
		dataFile.Close()
		log.Info("aggMetrics loaded from file.")
	} else {
		log.Info("starting with fresh aggmetrics.")
	}

	go ms.stats()
	go ms.GC()
	return &ms
}

func (ms *AggMetrics) stats() {
	gometrics.Register("points_per_metric", points)
	points.Update(0)

	metricsActive := gometrics.NewGauge()
	gometrics.Register("metrics_active", metricsActive)
	for range time.Tick(time.Duration(1) * time.Second) {
		ms.Lock()
		l := len(ms.Metrics)
		ms.Unlock()
		metricsActive.Update(int64(l))
	}
}

// periodically scan chunks and close any that have not recieved data in a while
func (ms *AggMetrics) GC() {
	ticker := time.Tick(time.Duration(int(ms.chunkSpan)) * time.Second)
	for now := range ticker {
		log.Info("checking for stale chunks that need persisting.")
		minTs := uint32(now.Unix()) - (ms.chunkSpan * ms.numChunks)
		ms.Lock()
		for key, a := range ms.Metrics {
			if stale := a.GC(minTs); stale {
				log.Info("metric %s is stale. Purging data from memory.", key)
				delete(ms.Metrics, key)
			}
		}
		ms.Unlock()
	}
}

func (ms *AggMetrics) Get(key string) (Metric, bool) {
	ms.Lock()
	m, ok := ms.Metrics[key]
	ms.Unlock()
	return m, ok
}

func (ms *AggMetrics) GetOrCreate(key string) Metric {
	ms.Lock()
	m, ok := ms.Metrics[key]
	if !ok {
		//m = NewAggMetric(key, ms.chunkSpan, ms.numChunks, aggSetting{ms.aggSpan, ms.aggChunkSpan, ms.aggNumChunks})
		m = NewAggMetric(key, ms.chunkSpan, ms.numChunks)
		ms.Metrics[key] = m
	}
	ms.Unlock()
	return m
}

func (ms *AggMetrics) Persist() error {
	// create a file\
	log.Info("persisting aggmetrics to disk.")
	dataFile, err := os.Create(*dumpFile)
	defer dataFile.Close()
	if err != nil {
		return err
	}

	dataEncoder := gob.NewEncoder(dataFile)
	err = dataEncoder.Encode(*ms)
	if err != nil {
		log.Error(0, "failed to encode aggMetrics to binary format. %s", err)
	} else {
		log.Info("successfully persisted aggMetrics to disk.")
	}
	return nil
}

type aggMetricsOnDisk struct {
	Metrics map[string]*AggMetric
}

func (a AggMetrics) GobEncode() ([]byte, error) {
	aOnDisk := aggMetricsOnDisk{
		Metrics: a.Metrics,
	}
	var b bytes.Buffer
	enc := gob.NewEncoder(&b)
	err := enc.Encode(aOnDisk)

	return b.Bytes(), err
}

func (a *AggMetrics) GobDecode(data []byte) error {
	r := bytes.NewReader(data)
	dec := gob.NewDecoder(r)
	aOnDisk := &aggMetricsOnDisk{}
	err := dec.Decode(aOnDisk)
	if err != nil {
		return err
	}
	a.Metrics = aOnDisk.Metrics
	return nil
}
