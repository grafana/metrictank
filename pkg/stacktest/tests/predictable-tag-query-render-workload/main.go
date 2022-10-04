package main

import (
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/grafana/metrictank/pkg/logger"
	"github.com/grafana/metrictank/pkg/stacktest/graphite"
	log "github.com/sirupsen/logrus"
)

var (
	key    = os.Getenv("API_KEY")
	base   = os.Getenv("API_ENDPOINT")
	bearer = fmt.Sprintf("Bearer " + key)
)

func init() {
	formatter := &logger.TextFormatter{}
	formatter.TimestampFormat = "2006-01-02 15:04:05.000"
	log.SetFormatter(formatter)
	log.SetLevel(log.InfoLevel)
}

func main() {
	go constantBackgroundLoad()
	intermittentConcurrentLoad()
}

// if we have errors, reduce rate. otherwise do 1 request at a time without rest. this is a good workload for now
func constantBackgroundLoad() {
	url := fmt.Sprintf("%s/render?target=seriesByTag('region=west100')&format=json&from=-48h", base)
	req, _ := http.NewRequest("GET", url, nil)
	req.Header.Add("Authorization", bearer)

	for {
		resp := graphite.ExecuteRenderQuery(req)
		if !graphite.ValidateTargets([]string{fakemetricName(100)}).Fn(resp) {
			log.Error("bad response: ", resp.StringWithoutData())
			time.Sleep(time.Second)
			continue
		}
	}
}

// issue a low rate of queries that access the same data concurrently
func intermittentConcurrentLoad() {
	q1 := "seriesByTag('thirdkey=~onemorevalue123456.*')"
	q2 := "seriesByTag('thirdkey=~onemorevalue1234567')"
	q3 := "seriesByTag('thirdkey=~onemorevalue1234567', 'region=west1234567')"
	q4 := "seriesByTag('thirdkey=~onemorevalue1234567', 'region!=foo')"
	q5 := "seriesByTag('thirdkey=onemorevalue1234567', 'region=~.*1234')"

	req1, _ := http.NewRequest("GET", fmt.Sprintf("%s/render?target=%s&format=json&from=-5min", base, url.QueryEscape(q1)), nil)
	req1.Header.Add("Authorization", bearer)

	req2, _ := http.NewRequest("GET", fmt.Sprintf("%s/render?target=%s&format=json&from=-5min", base, url.QueryEscape(q2)), nil)
	req2.Header.Add("Authorization", bearer)

	req3, _ := http.NewRequest("GET", fmt.Sprintf("%s/render?target=%s&format=json&from=-5min", base, url.QueryEscape(q3)), nil)
	req3.Header.Add("Authorization", bearer)

	req4, _ := http.NewRequest("GET", fmt.Sprintf("%s/render?target=%s&format=json&from=-5min", base, url.QueryEscape(q4)), nil)
	req4.Header.Add("Authorization", bearer)

	req5, _ := http.NewRequest("GET", fmt.Sprintf("%s/render?target=%s&format=json&from=-5min", base, url.QueryEscape(q5)), nil)
	req5.Header.Add("Authorization", bearer)

	ids := []int{1234560, 1234561, 1234562, 1234563, 1234564, 1234565, 1234566, 1234567, 1234568, 1234569, 123456}
	expectedNames := make([]string, len(ids))
	for i, id := range ids {
		expectedNames[i] = fakemetricName(id)
	}

	rand.Seed(time.Now().UnixNano())
	tick := time.Tick(time.Second)

	for range tick {
		randDur1 := time.Duration(math.Abs(rand.NormFloat64())*1000) * 1000000
		randDur2 := time.Duration(math.Abs(rand.NormFloat64())*1000) * 1000000
		randDur3 := time.Duration(math.Abs(rand.NormFloat64())*1000) * 1000000
		randDur4 := time.Duration(math.Abs(rand.NormFloat64())*1000) * 1000000
		randDur5 := time.Duration(math.Abs(rand.NormFloat64())*1000) * 1000000

		time.AfterFunc(randDur1, func() {
			resp := graphite.ExecuteRenderQuery(req1)
			if !graphite.ValidateTargets(expectedNames).Fn(resp) {
				log.Error("intermittentConcurrentLoad case 1 bad response", resp.StringWithoutData())
			}
		})
		time.AfterFunc(randDur2, func() {
			resp := graphite.ExecuteRenderQuery(req2)
			if !graphite.ValidateTargets(expectedNames[7:8]).Fn(resp) {
				log.Error("intermittentConcurrentLoad case 2 bad response", resp.StringWithoutData())
			}
		})
		time.AfterFunc(randDur3, func() {
			resp := graphite.ExecuteRenderQuery(req3)
			if !graphite.ValidateTargets(expectedNames[7:8]).Fn(resp) {
				log.Error("intermittentConcurrentLoad case 3 bad response", resp.StringWithoutData())
			}
		})
		time.AfterFunc(randDur4, func() {
			resp := graphite.ExecuteRenderQuery(req4)
			if !graphite.ValidateTargets(expectedNames[7:8]).Fn(resp) {
				log.Error("intermittentConcurrentLoad case 4 bad response", resp.StringWithoutData())
			}
		})
		time.AfterFunc(randDur5, func() {
			resp := graphite.ExecuteRenderQuery(req5)
			if !graphite.ValidateTargets(expectedNames[7:8]).Fn(resp) {
				log.Error("intermittentConcurrentLoad case 5 bad response", resp.StringWithoutData())
			}
		})
	}
}

func fakemetricName(num int) string {
	return fmt.Sprintf("some.id.of.a.metric.%d;afewmoretags=forgoodmeasure;anothertag=somelongervalue;goodforpeoplewhojustusetags=forbasicallyeverything;lotsandlotsoftags=morefunforeveryone;manymoreother=lotsoftagstointern;onetwothreefourfivesix=seveneightnineten;os=ubuntu;region=west%d;secondkey=anothervalue%d;thirdkey=onemorevalue%d", num, num, num, num)
}
