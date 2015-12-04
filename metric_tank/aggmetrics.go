package main

import (
	"bytes"
	"encoding/gob"
	"os"
	"sync"
	"time"

	"github.com/grafana/grafana/pkg/log"
)

type AggMetrics struct {
	sync.RWMutex
	Metrics        map[string]*AggMetric
	chunkSpan      uint32
	numChunks      uint32
	aggSettings    []aggSetting // for now we apply the same settings to all AggMetrics. later we may want to have different settings.
	chunkMaxStale  uint32
	metricMaxStale uint32
}

func NewAggMetrics(chunkSpan, numChunks, chunkMaxStale, metricMaxStale uint32, aggSettings []aggSetting) *AggMetrics {
	ms := AggMetrics{
		Metrics:        make(map[string]*AggMetric),
		chunkSpan:      chunkSpan,
		numChunks:      numChunks,
		aggSettings:    aggSettings,
		chunkMaxStale:  chunkMaxStale,
		metricMaxStale: metricMaxStale,
	}
	// open data file
	dataFile, err := os.Open(*dumpFile)

	if false && err == nil {
		log.Info("loading aggMetrics from file " + *dumpFile)
		dataDecoder := gob.NewDecoder(dataFile)
		err = dataDecoder.Decode(&ms)
		if err != nil {
			log.Error(3, "failed to load aggMetrics from file. %s", err)
		}
		dataFile.Close()
		log.Info("aggMetrics loaded from file.")
		if ms.numChunks != numChunks {
			if ms.numChunks > numChunks {
				log.Fatal(3, "numChunks can not be decreased.")
			}
			log.Info("numChunks has changed. Updating memory structures.")
			sem := make(chan bool, *concurrency)
			for _, m := range ms.Metrics {
				sem <- true
				go func() {
					m.GrowNumChunks(numChunks)
					<-sem
				}()
			}
			for i := 0; i < cap(sem); i++ {
				sem <- true
			}

			ms.numChunks = numChunks
			log.Info("memory structures updated.")
		}
	} else {
		log.Info("starting with fresh aggmetrics.")
	}

	go ms.stats()
	go ms.GC()
	return &ms
}

func (ms *AggMetrics) stats() {
	pointsPerMetric.Value(0)

	for range time.Tick(time.Duration(1) * time.Second) {
		ms.RLock()
		l := len(ms.Metrics)
		ms.RUnlock()
		metricsActive.Value(int64(l))
	}
}

// periodically scan chunks and close any that have not received data in a while
// TODO instrument occurences and duration of GC
func (ms *AggMetrics) GC() {
	ticker := time.Tick(time.Duration(*gcInterval) * time.Second)
	for now := range ticker {
		log.Info("checking for stale chunks that need persisting.")
		now := uint32(now.Unix())
		chunkMinTs := now - (now % ms.chunkSpan) - uint32(ms.chunkMaxStale)
		metricMinTs := now - (now % ms.chunkSpan) - uint32(ms.metricMaxStale)

		// as this is the only goroutine that can delete from ms.Metrics
		// we only need to lock long enough to get the list of actives metrics.
		// it doesnt matter if new metrics are added while we iterate this list.
		ms.RLock()
		keys := make([]string, 0, len(ms.Metrics))
		for k := range ms.Metrics {
			keys = append(keys, k)
		}
		ms.RUnlock()
		for _, key := range keys {
			ms.RLock()
			a := ms.Metrics[key]
			ms.RUnlock()
			if stale := a.GC(chunkMinTs, metricMinTs); stale {
				log.Info("metric %s is stale. Purging data from memory.", key)
				delete(ms.Metrics, key)
			}
		}

	}
}

func (ms *AggMetrics) Get(key string) (Metric, bool) {
	ms.RLock()
	m, ok := ms.Metrics[key]
	ms.RUnlock()
	return m, ok
}

func (ms *AggMetrics) GetOrCreate(key string) Metric {
	ms.Lock()
	m, ok := ms.Metrics[key]
	if !ok {
		m = NewAggMetric(key, ms.chunkSpan, ms.numChunks, ms.aggSettings...)
		ms.Metrics[key] = m
	}
	ms.Unlock()
	return m
}

// Persist saves the AggMetrics to disk.
func (ms *AggMetrics) Persist() error {
	return nil
	// create a file\
	log.Info("persisting aggmetrics to disk.")
	dataFile, err := os.Create(*dumpFile)
	defer dataFile.Close()
	if err != nil {
		return err
	}

	dataEncoder := gob.NewEncoder(dataFile)
	ms.RLock()
	err = dataEncoder.Encode(*ms)
	ms.RUnlock()
	if err != nil {
		log.Error(3, "failed to encode aggMetrics to binary format. %s", err)
	} else {
		log.Info("successfully persisted aggMetrics to disk.")
	}
	return nil
}

type aggMetricsOnDisk struct {
	Metrics   map[string]*AggMetric
	NumChunks uint32
}

func (a AggMetrics) GobEncode() ([]byte, error) {
	aOnDisk := aggMetricsOnDisk{
		Metrics:   a.Metrics,
		NumChunks: a.numChunks,
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
	a.numChunks = aOnDisk.NumChunks
	return nil
}
