package metricstore

import (
	"time"
	"github.com/raintank/raintank-metric/metricdef"
	"github.com/ctdk/goas/v2/logger"
)

type MetricStore struct {
	KairosDB *Kairosdb
	InfluxDB *Influxdb
}

func NewMetricStore(GraphiteAddr string, GraphitePort int, KairosdbHostPort string) (*MetricStore, error) {
	
	kairosdb, err := NewKairosdb(KairosdbHostPort)
	if err != nil {
		return nil, err
	}

	influx, err := NewInfluxdb(GraphiteAddr, GraphitePort)
	if err != nil {
		return nil, err
	}
	mStore := MetricStore{
		KairosDB: kairosdb,
		InfluxDB: influx,
	}
	return &mStore, nil
}


func (mStore MetricStore) ProcessBuffer(c <-chan metricdef.IndvMetric, workerId int) {
	buf := make([]metricdef.IndvMetric, 0)

	// flush buffer every second
	t := time.NewTicker(time.Second)
	for {
		select {
		case b := <-c:
			if b.Name != "" {
				logger.Debugf("worker %d appending to buffer", workerId)
				buf = append(buf, b)
			}
		case <-t.C:
			// A possibility: it might be worth it to hack up the
			// carbon lib to allow batch submissions of metrics if
			// doing them individually proves to be too slow

			//copy contents of buffer
			currentBuf := make([]metricdef.IndvMetric, len(buf))
			copy(currentBuf, buf)
			buf = nil
			logger.Debugf("worker %d flushing %d items in buffer now", workerId, len(currentBuf))
			if err := mStore.InfluxDB.SendMetrics(&currentBuf); err != nil {
				logger.Errorf(err.Error())
			}
			logger.Debugf("worker %d flushed metrics buffer to Influxdb", workerId)
			if err := mStore.KairosDB.SendMetrics(&currentBuf); err != nil {
				logger.Errorf(err.Error())
			}
			logger.Debugf("worker %d flushed metrics buffer to Kairosdb", workerId)
		}
	}
}