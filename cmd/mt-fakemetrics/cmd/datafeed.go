package cmd

import (
	"fmt"
	"time"

	"github.com/grafana/metrictank/cmd/mt-fakemetrics/metricbuilder"
	"github.com/grafana/metrictank/cmd/mt-fakemetrics/out"
	"github.com/grafana/metrictank/cmd/mt-fakemetrics/policy"
	"github.com/grafana/metrictank/schema"
	"github.com/raintank/worldping-api/pkg/log"
)

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
func dataFeed(out out.Out, orgs, mpo, period, flush, offset, speedup int, stopAtNow bool, builder metricbuilder.Builder, vp policy.ValuePolicy) {
	flushDur := time.Duration(flush) * time.Millisecond

	if mpo*speedup%period != 0 {
		panic("not a good fit. mpo*speedup must divide by period, to compute clean rate/s/org")
	}
	ratePerSPerOrg := mpo * speedup / period

	if mpo*speedup*flush%(1000*period) != 0 {
		panic("not a good fit. mpo*speedup*flush must divide by period, to compute clean rate/flush/org")
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

	tmpl := `params: %s, orgs=%d, mpo=%d, period=%d, flush=%d, offset=%d, speedup=%d, stopAtNow=%t
per org:         each %s, flushing %d metrics so rate of %d Hz. (%d total unique series)
times %4d orgs: each %s, flushing %d metrics so rate of %d Hz. (%d total unique series)
`
	fmt.Printf(tmpl, builder.Info(), orgs, mpo, period, flush, offset, speedup, stopAtNow,
		flushDur, ratePerFlush, ratePerS, orgs*mpo,
		orgs, flushDur, ratePerFlush, ratePerS, orgs*mpo)

	tick := time.NewTicker(flushDur)

	metrics := builder.Build(orgs, mpo, period)

	mp := int64(period)
	ts := time.Now().Unix() - int64(offset) - mp
	startingTs := ts
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
			ts = startingTs
		METRICS:
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
					if ts >= now && stopAtNow {
						break METRICS
					}
				}
				metricData.Time = ts
				metricData.Value = vp.Value(ts)

				data = append(data, &metricData)
			}
			startFrom = (m + 1) % mpo
		}

		preFlush := time.Now()
		err := out.Flush(data)
		if err != nil {
			log.Error(0, err.Error())
		}
		flushDuration.Value(time.Since(preFlush))

		log.ConsoleErrorf("ts: %v - now: %v - nowT: %v", ts, now, nowT)
		if ts >= now && stopAtNow {
			return
		}
	}
}
