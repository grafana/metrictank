package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strings"

	"net/http"
	_ "net/http/pprof"
	"time"

	"github.com/raintank/dur"
	"github.com/raintank/met"
	"github.com/raintank/met/helper"
	"github.com/raintank/raintank-metric/fake_metrics/out"
	"github.com/raintank/raintank-metric/fake_metrics/out/carbon"
	"github.com/raintank/raintank-metric/fake_metrics/out/gnet"
	"github.com/raintank/raintank-metric/fake_metrics/out/kafkamdam"
	"github.com/raintank/raintank-metric/fake_metrics/out/kafkamdm"
	"github.com/raintank/raintank-metric/fake_metrics/out/nsq"
	"github.com/raintank/schema"
	"github.com/raintank/worldping-api/pkg/log"
)

var (
	outs        []out.Out
	showVersion = flag.Bool("version", false, "print version string")
	topic       = flag.String("topic", "metrics", "NSQ topic")

	listenAddr       = flag.String("listen", ":6764", "http listener address for pprof.")
	nsqdTCPAddr      = flag.String("nsqd-tcp-address", "", "nsqd TCP address. e.g. localhost:4150")
	kafkaMdmTCPAddr  = flag.String("kafka-mdm-tcp-address", "", "kafka TCP address for MetricData-Msgp messages. e.g. localhost:9092")
	kafkaMdamTCPAddr = flag.String("kafka-mdam-tcp-address", "", "kafka TCP address for MetricDataArray-Msgp messages. e.g. localhost:9092")
	carbonTCPAddr    = flag.String("carbon-tcp-address", "", "carbon TCP address. e.g. localhost:2003")
	gnetAddr         = flag.String("gnet-address", "", "gnet address. e.g. http://localhost:8081")
	gnetKey          = flag.String("gnet-key", "", "gnet api key")
	kafkaCompression = flag.String("kafka-comp", "none", "compression: none|gzip|snappy")
	logLevel         = flag.Int("log-level", 2, "log level. 0=TRACE|1=DEBUG|2=INFO|3=WARN|4=ERROR|5=CRITICAL|6=FATAL")
	orgs             = flag.Int("orgs", 1, "how many orgs to simulate")
	keysPerOrg       = flag.Int("keys-per-org", 100, "how many metrics per orgs to simulate")
	speedup          = flag.Int("speedup", 1, "for each advancement of real time, how many advancements of fake data to simulate")
	metricPeriod     = flag.Int("metricPeriod", 1, "period in seconds between metric points")
	flushPeriod      = flag.Int("flushPeriod", 100, "period in ms between flushes. metricPeriod must be cleanly divisible by flushPeriod. does not affect volume/throughput per se. the message is adjusted as to keep the volume/throughput constant")
	offset           = flag.String("offset", "0", "offset duration expression. (how far back in time to start. e.g. 1month, 6h, etc")
	stopAtNow        = flag.Bool("stop-at-now", false, "stop program instead of starting to write data with future timestamps")
	statsdAddr       = flag.String("statsd-addr", "", "statsd address. e.g. localhost:8125")
	statsdType       = flag.String("statsd-type", "standard", "statsd type: standard or datadog")

	flushDuration met.Timer
)

func main() {
	flag.Parse()

	log.NewLogger(0, "console", fmt.Sprintf(`{"level": %d, "formatting":true}`, *logLevel))

	if *showVersion {
		fmt.Println("fake_metrics")
		return
	}

	if *carbonTCPAddr == "" && *gnetAddr == "" && *kafkaMdmTCPAddr == "" && *kafkaMdamTCPAddr == "" && *nsqdTCPAddr == "" {
		log.Fatal(4, "must use at least either carbon, gnet, kafka-mdm, kafka-mdam or nsq")
	}

	hostname, err := os.Hostname()
	if err != nil {
		log.Fatal(4, "failed to lookup hostname. %s", err)
	}
	var stats met.Backend
	if *statsdAddr == "" {
		stats, err = helper.New(false, *statsdAddr, *statsdType, "fake_metrics", strings.Replace(hostname, ".", "_", -1))
	} else {
		stats, err = helper.New(true, *statsdAddr, *statsdType, "fake_metrics", strings.Replace(hostname, ".", "_", -1))
	}
	if err != nil {
		log.Fatal(4, "failed to initialize statsd. %s", err)
	}
	flushDuration = stats.NewTimer("metricpublisher.global.flush_duration", 0)

	if *carbonTCPAddr != "" {
		if *orgs > 1 {
			log.Fatal(4, "can only simulate 1 org when using carbon output")
		}
		o, err := carbon.New(*carbonTCPAddr, stats)
		if err != nil {
			log.Fatal(4, "failed to create carbon output. %s", err)
		}
		outs = append(outs, o)
	}

	if *gnetAddr != "" {
		if *orgs > 1 {
			log.Fatal(4, "can only simulate 1 org when using gnet output")
		}
		if *gnetKey == "" {
			log.Fatal(4, "to use gnet, a key must be specified")
		}
		o, err := gnet.New(*gnetAddr, *gnetKey, stats)
		if err != nil {
			log.Fatal(4, "failed to create gnet output. %s", err)
		}
		outs = append(outs, o)
	}

	if *kafkaMdmTCPAddr != "" {
		o, err := kafkamdm.New("mdm", []string{*kafkaMdmTCPAddr}, *kafkaCompression, stats)
		if err != nil {
			log.Fatal(4, "failed to create kafka-mdm output. %s", err)
		}
		outs = append(outs, o)
	}

	if *kafkaMdamTCPAddr != "" {
		o, err := kafkamdam.New("mdam", []string{*kafkaMdamTCPAddr}, *kafkaCompression, stats)
		if err != nil {
			log.Fatal(4, "failed to create kafka-mdam output. %s", err)
		}
		outs = append(outs, o)
	}

	if *nsqdTCPAddr != "" {
		if *topic == "" {
			log.Fatal(4, "--topic is required for NSQ")
		}

		o, err := nsq.New(*topic, *nsqdTCPAddr, stats)
		if err != nil {
			log.Fatal(4, "failed to create NSQ output. %s", err)
		}
		outs = append(outs, o)
	}

	if (1000**metricPeriod)%*flushPeriod != 0 {
		panic("metricPeriod must be cleanly divisible by flushPeriod")
	}
	off := int(dur.MustParseUsec("offset", *offset))

	if *listenAddr != "" {
		go func() {
			log.Info("starting listener on %s", *listenAddr)
			err := http.ListenAndServe(*listenAddr, nil)
			if err != nil {
				log.Error(0, "%s", err)
			}
		}()
	}

	run(*orgs, *keysPerOrg, *metricPeriod, *flushPeriod, off, *speedup)
}

func run(orgs, keysPerOrg, metricPeriod, flushPeriod, offset, speedup int) {
	// examples:
	// flushperiod - metricperiod - speedup -> data to flush each flush
	// 100     ms    1 s            1          1/10 -> flush 1 of 10 fractions runDivided <-- default
	// 100     ms    1 s            2          1/5 -> flush 1 of 5 fractions   runDivided
	// 100     ms    1 s            100        1/5 -> flush 1x                 runMultiplied
	// 1000    ms    1 s            2          2x -> flush 2x                  runMultiplied
	// 2000    ms    1 s            1          2x -> flush 2x                  runMultiplied
	// 2000    ms    1 s            2          4x -> flush 4x                  runMultiplied

	if flushPeriod*speedup >= 1000*metricPeriod {
		runMultiplied(orgs, keysPerOrg, metricPeriod, flushPeriod, offset, speedup)
	} else {
		runDivided(orgs, keysPerOrg, metricPeriod, flushPeriod, offset, speedup)
	}
}

// we need to send multiples of the data at every flush
func runMultiplied(orgs, keysPerOrg, metricPeriod, flushPeriod, offset, speedup int) {
	if flushPeriod*speedup%(1000*metricPeriod) != 0 {
		panic(fmt.Sprintf("not a good fit. metricPeriod*1000 should fit in flushPeriod*speedup"))
	}
	ratio := flushPeriod * speedup / 1000 / metricPeriod
	uniqueKeys := orgs * keysPerOrg
	totalKeys := uniqueKeys * ratio
	tickDur := time.Duration(flushPeriod) * time.Millisecond
	tick := time.NewTicker(tickDur)
	fmt.Printf("each %s, sending %d metrics. (sending the %d metrics %d times per flush, each time advancing them by %d s)\n", tickDur, totalKeys, uniqueKeys, ratio, metricPeriod)

	metrics := make([]*schema.MetricData, totalKeys)
	for r := 1; r <= ratio; r++ {
		for o := 1; o <= orgs; o++ {
			for k := 1; k <= keysPerOrg; k++ {
				i := (r-1)*orgs*keysPerOrg + (o-1)*keysPerOrg + k - 1
				metrics[i] = &schema.MetricData{
					Name:       fmt.Sprintf("some.id.of.a.metric.%d", k),
					Metric:     "some.id.of.a.metric",
					OrgId:      o,
					Interval:   metricPeriod,
					Value:      0,
					Unit:       "ms",
					TargetType: "gauge",
					Tags:       []string{"some_tag", "ok"},
				}
				metrics[i].SetId()
			}
		}
	}
	mp := int64(metricPeriod)
	ts := time.Now().Unix() - int64(offset) - mp

	for range tick.C {
		now := time.Now().Unix()
		for i := 0; i < len(metrics); i++ {
			if i%uniqueKeys == 0 {
				ts += mp
			}
			metrics[i].Time = ts
			metrics[i].Value = rand.Float64() * float64(i+1)
		}

		preFlush := time.Now()
		for _, out := range outs {
			err := out.Flush(metrics)
			if err != nil {
				log.Error(0, err.Error())
			}
		}
		flushDuration.Value(time.Since(preFlush))

		if ts >= now && *stopAtNow {
			return
		}
	}
}

// we need to send fractions of the data at every flush
func runDivided(orgs, keysPerOrg, metricPeriod, flushPeriod, offset, speedup int) {
	totalKeys := orgs * keysPerOrg
	fractions := 1000 * metricPeriod / flushPeriod / speedup
	if totalKeys%fractions != 0 {
		panic(fmt.Sprintf("not a good fit: (%d orgs * %d keysPerOrg)=%d metrics but 1000*%d metricPeriod / %d flushPeriod / %d speedup = %d fractions)", orgs, keysPerOrg, totalKeys, metricPeriod, flushPeriod, speedup, fractions))
	}
	metricsPerFrac := totalKeys / fractions
	tickDur := time.Duration(flushPeriod) * time.Millisecond
	tick := time.NewTicker(tickDur)
	fmt.Printf("each %s, sending %d out of %d metrics. timestamps increment by %d s every %d flushes)\n", tickDur, metricsPerFrac, totalKeys, metricPeriod, fractions)

	metrics := make([]*schema.MetricData, totalKeys)
	for o := 1; o <= orgs; o++ {
		for k := 1; k <= keysPerOrg; k++ {
			i := (o-1)*keysPerOrg + k - 1
			metrics[i] = &schema.MetricData{
				Name:       fmt.Sprintf("some.id.of.a.metric.%d", k),
				Metric:     "some.id.of.a.metric",
				OrgId:      o,
				Interval:   metricPeriod,
				Value:      0,
				Unit:       "ms",
				TargetType: "gauge",
				Tags:       []string{"some_tag", "ok"},
			}
			metrics[i].SetId()
		}
	}

	mp := int64(metricPeriod)
	ts := time.Now().Unix() - int64(offset) - mp
	endIndex, startIndex := 0, 0
	for range tick.C {
		now := time.Now().Unix()
		startIndex = endIndex
		if startIndex == len(metrics) {
			startIndex = 0
		}
		endIndex = startIndex + metricsPerFrac
		if startIndex == 0 {
			ts += mp
		}
		for i := startIndex; i < endIndex; i++ {
			metrics[i].Time = ts
			metrics[i].Value = rand.Float64() * float64(i+1)
		}

		preFlush := time.Now()
		for _, out := range outs {
			err := out.Flush(metrics[startIndex:endIndex])
			if err != nil {
				log.Error(0, err.Error())
			}
		}
		flushDuration.Value(time.Since(preFlush))

		if ts >= now && *stopAtNow {
			return
		}
	}
}
