package main

import (
	"fmt"
	"github.com/grafana/metrictank/clock"
	"github.com/grafana/metrictank/stacktest/graphite"
	"github.com/grafana/metrictank/stats"
	log "github.com/sirupsen/logrus"
	"math"
	"net/http"
	"strconv"
	"time"
)

var (
	httpError    = stats.NewCounter32("parrot.monitoring.error;error=http")
	invalidError = stats.NewCounter32("parrot.monitoring.error;error=invalid")
)

type seriesStats struct {
	lastTs uint32
	//the partition currently being checked
	nans int32
	//the sum of abs(value - ts) across the time series
	deltaSum float64
	//the number of timestamps where value != ts
	numNonMatching int32
	//tracks the last seen non-NaN time stamp (useful for lag
	lastSeen uint32
}

type partitionMetrics struct {
	//number of missing values for each series
	nanCount *stats.Gauge32
	//time since the last value was recorded
	lag *stats.Gauge32
	//total amount of drift between expected value and actual values
	deltaSum *stats.Gauge32
	//total number of entries where drift occurred
	nonMatching *stats.Gauge32
}

func monitor() {
	metricsBySeries := initMetricsBySeries()
	for tick := range clock.AlignedTickLossless(queryInterval) {

		query := graphite.ExecuteRenderQuery(buildRequest(tick))
		if query.HTTPErr != nil {
			httpError.Inc()
		}
		if query.DecodeErr != nil {
			invalidError.Inc()
		}

		for _, s := range query.Decoded {
			log.Infof("%d - %d", s.Datapoints[0].Ts, s.Datapoints[len(s.Datapoints)-1].Ts)
			partition, err := strconv.Atoi(s.Target)
			if err != nil {
				log.Debug("unable to parse partition", err)
				invalidError.Inc()
				continue
			}
			serStats := seriesStats{}
			serStats.lastTs = s.Datapoints[len(s.Datapoints)-1].Ts

			for _, dp := range s.Datapoints {

				if math.IsNaN(dp.Val) {
					serStats.nans += 1
					continue
				}
				serStats.lastSeen = dp.Ts
				if diff := dp.Val - float64(dp.Ts); diff != 0 {
					log.Debugf("partition=%d dp.Val=%f dp.Ts=%d diff=%f", partition, dp.Val, dp.Ts, diff)
					serStats.deltaSum += diff
					serStats.numNonMatching += 1
				}
			}

			metrics := metricsBySeries[partition]
			metrics.nanCount.Set(int(serStats.nans))
			metrics.lag.Set(int(serStats.lastTs - serStats.lastSeen))
			metrics.deltaSum.Set(int(serStats.deltaSum))
			metrics.nonMatching.Set(int(serStats.numNonMatching))
		}
	}
}

func initMetricsBySeries() []partitionMetrics {
	var metricsBySeries []partitionMetrics
	for p := 0; p < int(partitionCount); p++ {
		metrics := partitionMetrics{
			nanCount:    stats.NewGauge32(fmt.Sprintf("parrot.monitoring.nancount;partition=%d", p)),
			lag:         stats.NewGauge32(fmt.Sprintf("parrot.monitoring.lag;partition=%d", p)),
			deltaSum:    stats.NewGauge32(fmt.Sprintf("parrot.monitoring.deltaSum;partition=%d", p)),
			nonMatching: stats.NewGauge32(fmt.Sprintf("parrot.monitoring.nonMatching;partition=%d", p)),
		}
		metricsBySeries = append(metricsBySeries, metrics)
	}
	return metricsBySeries
}

func buildRequest(now time.Time) *http.Request {
	req, _ := http.NewRequest("GET", fmt.Sprintf("%s/render", gatewayAddress), nil)
	q := req.URL.Query()
	q.Set("target", "aliasByNode(parrot.testdata.*.generated.*, 2)")
	q.Set("from", strconv.Itoa(int(now.Add(-5*time.Minute).Unix())))
	q.Set("until", strconv.Itoa(int(now.Unix())))
	q.Set("format", "json")
	q.Set("X-Org-Id", strconv.Itoa(orgId))
	req.URL.RawQuery = q.Encode()
	if len(gatewayKey) != 0 {
		req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", gatewayKey))
	}
	return req
}
