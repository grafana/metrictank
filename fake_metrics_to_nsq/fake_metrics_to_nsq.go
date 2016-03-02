package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strings"

	"time"

	//"github.com/davecgh/go-spew/spew"
	"github.com/grafana/grafana/pkg/log"
	"github.com/nsqio/go-nsq"
	"github.com/raintank/met"
	"github.com/raintank/met/helper"
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
	nsqdTCPAddr = flag.String("nsqd-tcp-address", "localhost:4150", "nsqd TCP address")
	logLevel    = flag.Int("log-level", 2, "log level. 0=TRACE|1=DEBUG|2=INFO|3=WARN|4=ERROR|5=CRITICAL|6=FATAL")
	orgs        = flag.Int("orgs", 2000, "how many orgs to simulate")
	keysPerOrg  = flag.Int("keys-per-org", 100, "how many metrics per orgs to simulate")
	steps       = flag.Int("steps", 1, "for each advancement of real time, how many advancements of fake data to simulate")
	interval    = flag.Int("interval", 1, "interval in seconds")
	offset      = flag.Int("offset", 0, "offset in seconds (how far back in time to start. you can catch up with >1 steps)")
	statsdAddr  = flag.String("statsd-addr", "localhost:8125", "statsd address")
	statsdType  = flag.String("statsd-type", "standard", "statsd type: standard or datadog")
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
	run(*orgs, *keysPerOrg, *steps, *interval, *offset)
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

func run(orgs, keysPerOrg, steps, interval, offset int) {
	var tickDuration time.Duration
	// if the step is high, it's wasteful to wait a long time (a second or more) and then do big batches
	// in that case it's better to flush messages smaller messages more continously
	if steps >= 1000 {
		tickDuration = time.Duration(1000*100*interval/steps) * time.Millisecond
		steps = 100
	} else {
		tickDuration = time.Duration(100) * time.Millisecond
	}
	fmt.Println("tickDuration", tickDuration, "steps", steps)

	metricsPerStep := orgs * keysPerOrg
	total := metricsPerStep * steps
	metrics := make([]*schema.MetricData, total)
	for s := 1; s <= steps; s++ {
		for o := 1; o <= orgs; o++ {
			for k := 1; k <= keysPerOrg; k++ {
				i := (s-1)*metricsPerStep + (o-1)*keysPerOrg + k - 1
				metrics[i] = &schema.MetricData{
					Name:       fmt.Sprintf("some.id.of.a.metric.%d", k),
					Metric:     "some.id.of.a.metric",
					OrgId:      o,
					Interval:   interval,
					Value:      0,
					Unit:       "ms",
					TargetType: "gauge",
					Tags:       []string{"some_tag", "ok"},
				}
				metrics[i].SetId()
			}
		}
	}
	ts := time.Now().Add(-time.Duration(offset-interval) * time.Second).Unix()
	tick := time.NewTicker(tickDuration)
	for range tick.C {
		for i := range metrics {
			if i%metricsPerStep == 0 {
				ts += int64(interval)
			}
			metrics[i].Time = ts
			metrics[i].Value = rand.Float64() * float64(i+1)
		}
		Publish(metrics)
	}
}
