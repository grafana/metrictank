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
	decodeError  = stats.NewCounter32("parrot.monitoring.error;error=decode")
	invalidError = stats.NewCounter32("parrot.monitoring.error;error=invalid")
)

var metricsBySeries []partitionMetrics

type seriesStats struct {
	lastTs           uint32
	nans             int32   // the partition currently being checked - nope?
	deltaSum         float64 // sum of abs(value - ts) across the time series
	numNonMatching   int32   // number of timestamps where value != ts
	lastSeen         uint32  // the last seen non-NaN time stamp (useful for lag)
	correctNumPoints bool    // whether the expected number of points were received
	correctAlignment bool    // whether the last ts matches `now`
	correctSpacing   bool    // whether all points are sorted and 1 period apart
}

type partitionMetrics struct {
	nanCount         *stats.Gauge32 // the number of missing values for each series
	lag              *stats.Gauge32 // time since the last value was recorded
	deltaSum         *stats.Gauge32 // the total amount of drift between expected value and actual values
	nonMatching      *stats.Gauge32 // total number of entries where drift occurred
	correctNumPoints *stats.Bool    // whether the expected number of points were received
	correctAlignment *stats.Bool    // whether the last ts matches `now`
	correctSpacing   *stats.Bool    // whether all points are sorted and 1 period apart
}

func monitor() {
	initMetricsBySeries()
	for tick := range clock.AlignedTickLossless(queryInterval) {

		query := graphite.ExecuteRenderQuery(buildRequest(tick))
		if query.HTTPErr != nil {
			httpError.Inc()
			continue
		}
		if query.DecodeErr != nil {
			decodeError.Inc()
			continue
		}

		for _, s := range query.Decoded {
			processPartitionSeries(s, tick)
		}
		statsGraphite.Report(tick)
	}
}

func processPartitionSeries(s graphite.Series, now time.Time) {
	partition, err := strconv.Atoi(s.Target)
	if err != nil {
		log.Debug("unable to parse partition", err)
		invalidError.Inc()
		return
	}
	if len(s.Datapoints) < 2 {
		log.Debugf("partition has invalid number of datapoints: %d", len(s.Datapoints))
		invalidError.Inc()
		return
	}

	serStats := seriesStats{}
	serStats.lastTs = s.Datapoints[len(s.Datapoints)-1].Ts
	serStats.correctAlignment = int64(serStats.lastTs) == now.Unix()
	serStats.correctNumPoints = len(s.Datapoints) == int(lookbackPeriod/testMetricsInterval)+1
	serStats.correctSpacing = checkSpacing(s.Datapoints)

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
	lag := lastPublish - int64(serStats.lastSeen)
	metrics.lag.Set(int(lag))
	metrics.deltaSum.Set(int(serStats.deltaSum))
	metrics.nonMatching.Set(int(serStats.numNonMatching))
	metrics.correctNumPoints.Set(serStats.correctNumPoints)
	metrics.correctAlignment.Set(serStats.correctAlignment)
	metrics.correctSpacing.Set(serStats.correctSpacing)
}

func checkSpacing(points []graphite.Point) bool {
	previous := points[0].Ts
	for i := 1; i < len(points); i++ {
		current := points[i].Ts
		if current-previous != uint32(testMetricsInterval.Seconds()) {
			return false
		}
		previous = current
	}
	return true
}

func initMetricsBySeries() {
	for p := 0; p < int(partitionCount); p++ {
		metrics := partitionMetrics{
			// metric parrot.monitoring.nancount is the number of missing values for each series
			nanCount: stats.NewGauge32(fmt.Sprintf("parrot.monitoring.nancount;partition=%d", p)),
			// metric parrot.monitoring.lag is the time since the last value was recorded
			lag: stats.NewGauge32(fmt.Sprintf("parrot.monitoring.lag;partition=%d", p)),
			// metric parrot.monitoring.deltaSum is the total amount of drift between expected value and actual values
			deltaSum: stats.NewGauge32(fmt.Sprintf("parrot.monitoring.deltaSum;partition=%d", p)),
			// metric parrot.monitoring.nonMatching is the total number of entries where drift occurred
			nonMatching: stats.NewGauge32(fmt.Sprintf("parrot.monitoring.nonMatching;partition=%d", p)),
			// metric parrot.monitoring.correctNumPoints is whether the expected number of points were received
			correctNumPoints: stats.NewBool(fmt.Sprintf("parrot.monitoring.correctNumPoints;partition=%d", p)),
			// metric parrot.monitoring.correctAlignment is whether the last ts matches `now`
			correctAlignment: stats.NewBool(fmt.Sprintf("parrot.monitoring.correctAlignment;partition=%d", p)),
			// metric parrot.monitoring.correctSpacing is whether all points are sorted and 1 period apart
			correctSpacing: stats.NewBool(fmt.Sprintf("parrot.monitoring.correctSpacing;partition=%d", p)),
		}
		metricsBySeries = append(metricsBySeries, metrics)
	}
}

func buildRequest(now time.Time) *http.Request {
	req, _ := http.NewRequest("GET", fmt.Sprintf("%s/render", gatewayAddress), nil)
	q := req.URL.Query()
	q.Set("target", "aliasByNode(parrot.testdata.*.generated.*, 2)")
	q.Set("from", strconv.Itoa(int(now.Add(-1*lookbackPeriod).Unix()-1)))
	q.Set("until", strconv.Itoa(int(now.Unix())))
	q.Set("format", "json")
	q.Set("X-Org-Id", strconv.Itoa(orgId))
	req.URL.RawQuery = q.Encode()
	if len(gatewayKey) != 0 {
		req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", gatewayKey))
	}
	return req
}
