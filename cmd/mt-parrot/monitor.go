package main

import (
	"fmt"
	"math"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/grafana/metrictank/clock"
	"github.com/grafana/metrictank/stacktest/graphite"
	"github.com/grafana/metrictank/stats"
	"github.com/grafana/metrictank/util/align"
	log "github.com/sirupsen/logrus"
)

var (
	httpError    = stats.NewCounter32WithTags("parrot.monitoring.error", ";error=http")
	decodeError  = stats.NewCounter32WithTags("parrot.monitoring.error", ";error=decode")
	invalidError = stats.NewCounter32WithTags("parrot.monitoring.error", ";error=invalid")
)

var metricsBySeries []partitionMetrics

type seriesInfo struct {
	lastTs uint32 // last timestamp seen in the response

	// to generate stats from
	lastSeen         uint32  // the last seen non-NaN time stamp (to generate lag from)
	deltaSum         float64 // sum of abs(value - ts) across the time series
	numNans          int32   // number of missing values for each series
	numNonMatching   int32   // number of points where value != ts
	correctNumPoints bool    // whether the expected number of points were received
	correctAlignment bool    // whether the last ts matches `now`
	correctSpacing   bool    // whether all points are sorted and 1 period apart
}

type partitionMetrics struct {
	lag              *stats.Gauge32 // time since the last value was recorded
	deltaSum         *stats.Gauge32 // total amount of drift between expected value and actual values
	numNans          *stats.Gauge32 // number of missing values for each series
	numNonMatching   *stats.Gauge32 // number of points where value != ts
	correctNumPoints *stats.Bool    // whether the expected number of points were received
	correctAlignment *stats.Bool    // whether the last ts matches `now`
	correctSpacing   *stats.Bool    // whether all points are sorted and 1 period apart
}

func monitor() {
	initMetricsBySeries()
	for tick := range clock.AlignedTickLossy(queryInterval) {

		resp := graphite.ExecuteRenderQuery(buildRequest(tick))
		if resp.HTTPErr != nil {
			httpError.Inc()
			continue
		}
		if resp.DecodeErr != nil {
			decodeError.Inc()
			continue
		}

		for _, s := range resp.Decoded {
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

	serStats := seriesInfo{}
	lastTs := align.Forward(uint32(now.Unix()), uint32(testMetricsInterval.Seconds()))
	serStats.lastTs = s.Datapoints[len(s.Datapoints)-1].Ts
	serStats.correctAlignment = serStats.lastTs == lastTs
	serStats.correctNumPoints = len(s.Datapoints) == int(lookbackPeriod/testMetricsInterval)
	serStats.correctSpacing = checkSpacing(s.Datapoints)

	for _, dp := range s.Datapoints {
		if math.IsNaN(dp.Val) {
			serStats.numNans += 1
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
	metrics.numNans.Set(int(serStats.numNans))
	lag := atomic.LoadInt64(&lastPublish) - int64(serStats.lastSeen)
	metrics.lag.Set(int(lag))
	metrics.deltaSum.Set(int(serStats.deltaSum))
	metrics.numNonMatching.Set(int(serStats.numNonMatching))
	metrics.correctNumPoints.Set(serStats.correctNumPoints)
	metrics.correctAlignment.Set(serStats.correctAlignment)
	metrics.correctSpacing.Set(serStats.correctSpacing)
}

func checkSpacing(points []graphite.Point) bool {
	for i := 1; i < len(points); i++ {
		prev := points[i-1].Ts
		cur := points[i].Ts
		if cur-prev != uint32(testMetricsInterval.Seconds()) {
			return false
		}
	}
	return true
}

func initMetricsBySeries() {
	for p := 0; p < int(partitionCount); p++ {
		metrics := partitionMetrics{
			//TODO enable metrics2docs by adding 'metric' prefix to each metric
			// parrot.monitoring.nans is the number of missing values for each series
			numNans: stats.NewGauge32WithTags("parrot.monitoring.nans", fmt.Sprintf(";partition=%d", p)),
			// parrot.monitoring.lag is the time since the last value was recorded
			lag: stats.NewGauge32WithTags("parrot.monitoring.lag", fmt.Sprintf(";partition=%d", p)),
			// parrot.monitoring.deltaSum is the total amount of drift between expected value and actual values
			deltaSum: stats.NewGauge32WithTags("parrot.monitoring.deltaSum", fmt.Sprintf(";partition=%d", p)),
			// parrot.monitoring.nonmatching is the total number of entries where drift occurred
			numNonMatching: stats.NewGauge32WithTags("parrot.monitoring.nonmatching", fmt.Sprintf(";partition=%d", p)),
			// parrot.monitoring.correctNumPoints is whether the expected number of points were received
			correctNumPoints: stats.NewBoolWithTags("parrot.monitoring.correctNumPoints", fmt.Sprintf(";partition=%d", p)),
			// parrot.monitoring.correctAlignment is whether the last ts matches `now`
			correctAlignment: stats.NewBoolWithTags("parrot.monitoring.correctAlignment", fmt.Sprintf(";partition=%d", p)),
			// parrot.monitoring.correctSpacing is whether all points are sorted and 1 period apart
			correctSpacing: stats.NewBoolWithTags("parrot.monitoring.correctSpacing", fmt.Sprintf(";partition=%d", p)),
		}
		metricsBySeries = append(metricsBySeries, metrics)
	}
}

func buildRequest(now time.Time) *http.Request {
	req, _ := http.NewRequest("GET", fmt.Sprintf("%s/render", gatewayAddress), nil)
	q := req.URL.Query()
	q.Set("target", "aliasByNode(parrot.testdata.*.identity.*, 2)")
	q.Set("from", strconv.Itoa(int(now.Add(-1*lookbackPeriod).Unix())))
	q.Set("until", strconv.Itoa(int(now.Unix())))
	q.Set("format", "json")
	q.Set("X-Org-Id", strconv.Itoa(orgId))
	req.URL.RawQuery = q.Encode()
	if len(gatewayKey) != 0 {
		req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", gatewayKey))
	}
	return req
}
