package main

import (
	"bytes"
	"encoding/gob"
	gometrics "github.com/rcrowley/go-metrics"
	"log"
	"os"
	"sync"
	"time"
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
	dataFile, err := os.Open("aggmetrics.gob")

	if err == nil {
		log.Printf("loading aggmetrics from file.")
		dataDecoder := gob.NewDecoder(dataFile)
		err = dataDecoder.Decode(&ms)
		if err != nil {
			log.Printf("failed to load aggmetrics from file. %v", err)
		}
		dataFile.Close()
	} else {
		log.Printf("starting with fresh aggmetrics.")
	}

	go ms.stats()
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
	log.Println("persisting aggmetrics to disk.")
	dataFile, err := os.Create("aggmetrics.gob")

	if err != nil {
		return err
	}

	dataEncoder := gob.NewEncoder(dataFile)
	dataEncoder.Encode(*ms)

	dataFile.Close()
	log.Println("persisted aggmetrics to disk.")
	return nil
}

type aggMetricsOnDisk struct {
	Metrics map[string]*AggMetric
}

func (a AggMetrics) GobEncode() ([]byte, error) {
	log.Println("marshaling AggMetrics to Binary")
	aOnDisk := aggMetricsOnDisk{
		Metrics: a.Metrics,
	}
	var b bytes.Buffer
	enc := gob.NewEncoder(&b)
	err := enc.Encode(aOnDisk)

	return b.Bytes(), err
}

func (a *AggMetrics) GobDecode(data []byte) error {
	log.Println("unmarshaling AggMetrics to Binary")
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
