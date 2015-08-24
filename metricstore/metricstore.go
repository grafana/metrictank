package metricstore

import (
	"time"

	"github.com/ctdk/goas/v2/logger"
	"github.com/raintank/raintank-metric/schema"
	"github.com/raintank/raintank-metric/setting"
)

type MetricBackend interface {
	SendMetrics(*[]schema.MetricData) error
	Type() string
}

type MetricStore struct {
	Backends []MetricBackend
}

func NewMetricStore() (*MetricStore, error) {
	mStore := MetricStore{}
	if setting.Config.EnableKairosdb {
		kairosdb, err := NewKairosdb(setting.Config.KairosdbUrl)
		logger.Debugf("Adding kairosdb to list of backends.")
		if err != nil {
			return nil, err
		}
		mStore.Backends = append(mStore.Backends, kairosdb)
	}

	if setting.Config.EnableCarbon {
		carbon, err := NewCarbon(setting.Config.CarbonAddr, setting.Config.CarbonPort)
		logger.Debugf("Adding Carbon to list of backends.")
		if err != nil {
			return nil, err
		}
		mStore.Backends = append(mStore.Backends, carbon)
	}

	return &mStore, nil
}

func (mStore MetricStore) ProcessBuffer(c <-chan schema.MetricData, workerId int) {
	buf := make([]schema.MetricData, 0)

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
			currentBuf := make([]schema.MetricData, len(buf))
			copy(currentBuf, buf)
			buf = nil
			logger.Debugf("worker %d flushing %d items in buffer now", workerId, len(currentBuf))
			for _, backend := range mStore.Backends {
				if err := backend.SendMetrics(&currentBuf); err != nil {
					logger.Errorf(err.Error())
				} else {
					logger.Debugf("worker %d flushed metrics buffer to %s backend", workerId, backend.Type())
				}
			}
		}
	}
}
