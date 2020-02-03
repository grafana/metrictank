package cmd

import (
	"fmt"
	"math/rand"
	"strconv"
	"time"

	"github.com/grafana/metrictank/schema"
	"github.com/raintank/fakemetrics/out"
	"github.com/raintank/worldping-api/pkg/log"
)

// slice of MetricData's, per orgid
// with time and value not set yet.
func buildMetrics(metricName string, orgs, mpo, period int) [][]schema.MetricData {
	out := make([][]schema.MetricData, orgs)
	for o := 0; o < orgs; o++ {
		metrics := make([]schema.MetricData, mpo)
		for m := 0; m < mpo; m++ {
			var tags []string
			name := fmt.Sprintf("%s.%d", metricName, m+1)

			localTags := []string{
				"secondkey=anothervalue",
				"thirdkey=onemorevalue",
				"region=west",
				"os=ubuntu",
				"anothertag=somelongervalue",
				"manymoreother=lotsoftagstointern",
				"afewmoretags=forgoodmeasure",
				"onetwothreefourfivesix=seveneightnineten",
				"lotsandlotsoftags=morefunforeveryone",
				"goodforpeoplewhojustusetags=forbasicallyeverything",
			}

			if len(customTags) > 0 {
				if numUniqueCustomTags > 0 {
					var j int
					for j = 0; j < numUniqueCustomTags; j++ {
						tags = append(tags, customTags[j]+strconv.Itoa(m+1))
					}
					for j < len(customTags) {
						tags = append(tags, customTags[j])
						j++
					}

				} else {
					tags = customTags
				}
			}

			if addTags {
				if numUniqueTags > 0 {
					var j int
					for j = 0; j < numUniqueTags; j++ {
						tags = append(tags, localTags[j]+strconv.Itoa(m+1))
					}
					for j < len(localTags) {
						tags = append(tags, localTags[j])
						j++
					}
				} else {
					tags = localTags
				}
			}
			metrics[m] = schema.MetricData{
				Name:     name,
				OrgId:    o + 1,
				Interval: period,
				Unit:     "ms",
				Mtype:    "gauge",
				Tags:     tags,
			}
			metrics[m].SetId()
		}
		out[o] = metrics
	}
	return out
}

// examples (everything perOrg)
// num metrics - flush (s) - period (s) - speedup -> ratePerSPerOrg      -> ratePerFlushPerOrg
// mpo         -                                     mpo*speedup/period  -> mpo*speedup*flush/period
// mpo         - 0.1         1            1          mpo                    mpo/10           1/10 -> flush 1 of 10 fractions runDivided <-- default
// mpo         - 0.1         1            2          mpo*2                  mpo/5            1/5 -> flush 1 of 5 fractions   runDivided
// mpo         - 0.1         1            100        mpo*100                mpo*10           1/5 -> flush 1x                 runMultiplied
// mpo         - 1           1            2          mpo*2                  mpo*2            2x -> flush 2x                  runMultiplied
// mpo         - 2           1            1          mpo                    mpo*2            2x -> flush 2x                  runMultiplied
// mpo         - 2           1            2          mpo*2                  mpo*2            4x -> flush 4x                  runMultiplied

// dataFeed supports both realtime, as backfill, with speedup
// important:
// period in seconds
// flush  in ms
// offset in seconds
func dataFeed(outs []out.Out, metricName string, orgs, mpo, period, flush, offset, speedup int, stopAtNow bool) {
	flushDur := time.Duration(flush) * time.Millisecond

	if mpo*speedup%period != 0 {
		panic("not a good fit. mpo*speedup must fit in period, to compute clean rate/s/org")
	}
	ratePerSPerOrg := mpo * speedup / period

	if mpo*speedup*flush%(1000*period) != 0 {
		panic("not a good fit. mpo*speedup*flush must fit in period, to compute clean rate/flush/org")
	}
	ratePerFlushPerOrg := ratePerSPerOrg * flush / 1000

	if addTags && len(customTags) > 0 {
		panic("cannot use regular-tags and custom-tags at the same time")
	}

	if numUniqueTags > 10 || numUniqueTags < 0 {
		panic(fmt.Sprintf("num-unique-tags must be a value between 0 and 10, you entered %d", numUniqueTags))
	}

	if numUniqueCustomTags > len(customTags) || numUniqueCustomTags < 0 {
		panic(fmt.Sprintf("num-unique-custom-tags must be a value between 0 and %d, you entered %d", len(customTags), numUniqueCustomTags))
	}

	ratePerS := ratePerSPerOrg * orgs
	ratePerFlush := ratePerFlushPerOrg * orgs

	fmt.Printf("params: metricname=%s, orgs=%d, mpo=%d, period=%d, flush=%d, offset=%d, speedup=%d, stopAtNow=%t\n", metricName, orgs, mpo, period, flush, offset, speedup, stopAtNow)
	fmt.Printf("per org:         each %s, flushing %d metrics so rate of %d Hz. (%d total unique series)\n", flushDur, ratePerFlush, ratePerS, orgs*mpo)
	fmt.Printf("times %4d orgs: each %s, flushing %d metrics so rate of %d Hz. (%d total unique series)\n", orgs, flushDur, ratePerFlush, ratePerS, orgs*mpo)

	tick := time.NewTicker(flushDur)

	metrics := buildMetrics(metricName, orgs, mpo, period)

	mp := int64(period)
	ts := time.Now().Unix() - int64(offset) - mp
	startFrom := 0

	// huh what if we increment ts beyond the now ts?
	// this can only happen if we repeatedly loop, and bump ts each time
	// let's say we loop 5 times, so:
	// ratePerFlushPerOrg == 5 * mpo
	// then last ts = ts+4*period
	// (loops-1)*period < flush
	// (ceil(ratePerFlushPerOrg/mpo)-1)*period < flush
	// (ceil(mpo * speedup * flush /period /mpo)-1)*period < flush
	// (ceil(speedup * flush /period)-1)*period < flush
	// (ceil(speedup * flush - period ) < flush

	for nowT := range tick.C {
		now := nowT.Unix()
		var data []*schema.MetricData

		for o := 0; o < len(metrics); o++ {
			// as seen above, we need to flush ratePerFlushPerOrg
			// respecting where a previous flush left off, we need to start from
			// the point after it.
			var m int
			for num := 0; num < ratePerFlushPerOrg; num++ {
				// note that ratePerFlushPerOrg may be any of >, =, < mpo
				// it all depends on what the user requested
				// the main thing we need to watch out for here is to bump the timestamp
				// is properly bumped in both cases
				m = (startFrom + num) % mpo
				metricData := metrics[o][m]
				// not the very first metric, but we "cycled back" to reusing metrics
				// we already sent, so we must increase the timestamp
				if m == 0 {
					ts += mp
				}
				metricData.Time = ts
				metricData.Value = rand.Float64() * float64(m+1)
				data = append(data, &metricData)
			}
			startFrom = (m + 1) % mpo
		}

		preFlush := time.Now()
		for _, out := range outs {
			err := out.Flush(data)
			if err != nil {
				log.Error(0, err.Error())
			}
		}
		flushDuration.Value(time.Since(preFlush))

		if ts >= now && stopAtNow {
			return
		}
	}
}
