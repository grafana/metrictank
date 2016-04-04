package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strings"

	"time"

	"github.com/grafana/grafana/pkg/log"
	"github.com/nsqio/go-nsq"
	"github.com/raintank/met"
	"github.com/raintank/met/helper"
	"github.com/raintank/raintank-metric/dur"
	"github.com/raintank/raintank-metric/msg"
	"github.com/raintank/raintank-metric/schema"
)

const maxMpubSize = 5 * 1024 * 1024 // nsq errors if more. not sure if can be changed
const maxMetricPerMsg = 1000        // emperically found through benchmarks (should result in 64~128k messages)
var (
	producer          *nsq.Producer
	metricsPublished  met.Count
	messagesPublished met.Count
	messagesSize      met.Meter
	metricsPerMessage met.Meter
	publishDuration   met.Timer
)

var (
	showVersion = flag.Bool("version", false, "print version string")

	topic = flag.String("topic", "metrics", "NSQ topic")

	//producerOpts     = flag.String("producer-opt", "", "option to passthrough to nsq.Producer (may be given multiple times as comma-separated list, see http://godoc.org/github.com/nsqio/go-nsq#Config)")
	nsqdTCPAddr  = flag.String("nsqd-tcp-address", "localhost:4150", "nsqd TCP address")
	logLevel     = flag.Int("log-level", 2, "log level. 0=TRACE|1=DEBUG|2=INFO|3=WARN|4=ERROR|5=CRITICAL|6=FATAL")
	orgs         = flag.Int("orgs", 2000, "how many orgs to simulate")
	keysPerOrg   = flag.Int("keys-per-org", 100, "how many metrics per orgs to simulate")
	speedup      = flag.Int("speedup", 1, "for each advancement of real time, how many advancements of fake data to simulate")
	metricPeriod = flag.Int("metricPeriod", 1, "period in seconds between metric points")
	flushPeriod  = flag.Int("flushPeriod", 100, "period in ms between flushes. metricPeriod must be cleanly divisible by flushPeriod. does not affect volume/throughput per se. the message is adjusted as to keep the volume/throughput constant")
	offset       = flag.String("offset", "0", "offset duration expression. (how far back in time to start. e.g. 1month, 6h, etc")
	stopAtNow    = flag.Bool("stop-at-now", false, "stop program instead of starting to write data with future timestamps")
	statsdAddr   = flag.String("statsd-addr", "localhost:8125", "statsd address")
	statsdType   = flag.String("statsd-type", "standard", "statsd type: standard or datadog")
)

func main() {
	flag.Parse()

	log.NewLogger(0, "console", fmt.Sprintf(`{"level": %d, "formatting":true}`, *logLevel))

	if *showVersion {
		fmt.Println("fake_metrics_to_nsq")
		return
	}

	if *topic == "" {
		log.Fatal(4, "--topic is required")
	}

	hostname, err := os.Hostname()
	if err != nil {
		log.Fatal(4, "failed to lookup hostname. %s", err)
	}
	stats, err := helper.New(true, *statsdAddr, *statsdType, "fake_metrics_to_nsq", strings.Replace(hostname, ".", "_", -1))
	if err != nil {
		log.Fatal(4, "failed to initialize statsd. %s", err)
	}

	cfg := nsq.NewConfig()
	cfg.UserAgent = "fake_metrics_to_nsq"
	producer, err = nsq.NewProducer(*nsqdTCPAddr, cfg)
	if err != nil {
		log.Fatal(0, "failed to initialize nsq producer.", err)
	}
	err = producer.Ping()
	if err != nil {
		log.Fatal(0, "can't connect to nsqd: %s", err)
	}
	metricsPublished = stats.NewCount("metricpublisher.metrics-published")
	messagesPublished = stats.NewCount("metricpublisher.messages-published")
	messagesSize = stats.NewMeter("metricpublisher.message_size", 0)
	metricsPerMessage = stats.NewMeter("metricpublisher.metrics_per_message", 0)
	publishDuration = stats.NewTimer("metricpublisher.publish_duration", 0)
	if (1000**metricPeriod)%*flushPeriod != 0 {
		panic("metricPeriod must be cleanly divisible by flushPeriod")
	}
	off := int(dur.MustParseUsec("offset", *offset))
	run(*orgs, *keysPerOrg, *metricPeriod, *flushPeriod, off, *speedup)
}

func Reslice(in []*schema.MetricData, size int) [][]*schema.MetricData {
	numSubSlices := len(in) / size
	if len(in)%size > 0 {
		numSubSlices += 1
	}
	out := make([][]*schema.MetricData, numSubSlices)
	for i := 0; i < numSubSlices; i++ {
		start := i * size
		end := (i + 1) * size
		if end > len(in) {
			out[i] = in[start:]
		} else {
			out[i] = in[start:end]
		}
	}
	return out
}

func Publish(metrics []*schema.MetricData) error {
	if len(metrics) == 0 {
		return nil
	}
	// typical metrics seem to be around 300B
	// nsqd allows <= 10MiB messages.
	// we ideally have 64kB ~ 1MiB messages (see benchmark https://gist.github.com/Dieterbe/604232d35494eae73f15)
	// at 300B, about 3500 msg fit in 1MiB
	// in worst case, this allows messages up to 2871B
	// this could be made more robust of course

	// real world findings in dev-stack with env-load:
	// 159569B msg /795  metrics per msg = 200B per msg
	// so peak message size is about 3500*200 = 700k (seen 711k)

	subslices := Reslice(metrics, 3500)

	for _, subslice := range subslices {
		id := time.Now().UnixNano()
		data, err := msg.CreateMsg(subslice, id, msg.FormatMetricDataArrayMsgp)
		if err != nil {
			log.Fatal(0, "Fatal error creating metric message: %s", err)
		}
		metricsPublished.Inc(int64(len(subslice)))
		messagesPublished.Inc(1)
		messagesSize.Value(int64(len(data)))
		metricsPerMessage.Value(int64(len(subslice)))
		pre := time.Now()
		err = producer.Publish(*topic, data)
		publishDuration.Value(time.Since(pre))
		if err != nil {
			log.Fatal(0, "can't publish to nsqd: %s", err)
		}
		//log.Info("published metrics %d size=%d", id, len(data))
	}
	return nil
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
				if ts > now && *stopAtNow {
					return
				}

			}
			metrics[i].Time = ts
			metrics[i].Value = rand.Float64() * float64(i+1)
		}
		Publish(metrics)
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
			if ts > now && *stopAtNow {
				return
			}
		}
		for i := startIndex; i < endIndex; i++ {
			metrics[i].Time = ts
			metrics[i].Value = rand.Float64() * float64(i+1)
		}
		Publish(metrics[startIndex:endIndex])
	}
}
