package main

import (
	"fmt"
	"github.com/grafana/metrictank/stacktest/graphite"
	log "github.com/sirupsen/logrus"
	"math"
	"net/http"
	"strconv"
	"time"
)

type seriesStats struct {
	lastTs uint32
	//the partition currently being checked
	partition int32
	//the number of nans present in the time series
	nans int32
	//the sum of abs(value - ts) across the time series
	deltaSum float64
	//the number of timestamps where value != ts
	numNonMatching int32

	//tracks the last seen non-NaN time stamp (useful for lag
	lastSeen uint32
}

func monitor() {
	for range time.NewTicker(queryInterval).C {

		query := graphite.ExecuteRenderQuery(buildRequest())

		for _, s := range query.Decoded {
			log.Infof("%d - %d", s.Datapoints[0].Ts, s.Datapoints[len(s.Datapoints)-1].Ts)

			stats := seriesStats{}
			stats.lastTs = s.Datapoints[len(s.Datapoints)-1].Ts

			for _, dp := range s.Datapoints {

				if math.IsNaN(dp.Val) {
					stats.nans += 1
					continue
				}
				if diff := dp.Val - float64(dp.Ts); diff != 0 {
					stats.lastSeen = dp.Ts
					stats.deltaSum += diff
					stats.numNonMatching += 1
				}
			}

			//TODO create/emit metrics for each partition

			//number of missing values for each series
			fmt.Printf("parrot.monitoring.nanCount;partition=%d; %d\n", stats.partition, stats.nans)
			//time since the last value was recorded
			fmt.Printf("parrot.monitoring.lag;partition=%d; %d\n", stats.partition, stats.lastTs-stats.lastSeen)
			//total amount of drift between expected value and actual values
			fmt.Printf("parrot.monitoring.deltaSum;partition=%d; %f\n", stats.partition, stats.deltaSum)
			//total number of entries where drift occurred
			fmt.Printf("parrot.monitoring.nonMatching;partition=%d; %d\n", stats.partition, stats.numNonMatching)
			fmt.Println()
		}
	}
}

func buildRequest() *http.Request {
	req, _ := http.NewRequest("GET", fmt.Sprintf("%s/render", gatewayAddress), nil)
	q := req.URL.Query()
	q.Set("target", "parrot.testdata.*.generated.*")
	//TODO parameterize this
	q.Set("from", "-5min")
	q.Set("until", "now")
	q.Set("format", "json")
	q.Set("X-Org-Id", strconv.Itoa(orgId))
	req.URL.RawQuery = q.Encode()
	if len(gatewayKey) != 0 {
		req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", gatewayKey))
	}
	return req
}
