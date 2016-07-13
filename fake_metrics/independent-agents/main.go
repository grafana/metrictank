package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strings"

	"time"

	"github.com/raintank/met"
	"github.com/raintank/met/helper"
	"github.com/raintank/raintank-metric/fake_metrics/out"
	"github.com/raintank/raintank-metric/fake_metrics/out/carbon"
	"github.com/raintank/raintank-metric/fake_metrics/out/kafkamdm"
	"github.com/raintank/raintank-metric/fake_metrics/out/nsq"
	"github.com/raintank/raintank-metric/fake_metrics/out/stdout"
	"github.com/raintank/schema"
	"github.com/raintank/worldping-api/pkg/log"
)

var (
	showVersion = flag.Bool("version", false, "print version string")
	topic       = flag.String("topic", "metrics", "NSQ topic")
	std         out.Out // global resource because needs to be globally locked. os.Stdout is not threadsafe

	nsqdTCPAddr      = flag.String("nsqd-tcp-address", "", "nsqd TCP address. e.g. localhost:4150")
	kafkaMdmTCPAddr  = flag.String("kafka-mdm-tcp-address", "", "kafka TCP address for MetricDataArray-Msgp messages. e.g. localhost:9092")
	kafkaCompression = flag.String("kafka-comp", "none", "compression: none|gzip|snappy")
	carbonTCPAddr    = flag.String("carbon-tcp-address", "", "carbon TCP address. e.g. localhost:2003")
	stdoutOut        = flag.Bool("stdout", false, "enable emitting metrics to stdout")
	logLevel         = flag.Int("log-level", 2, "log level. 0=TRACE|1=DEBUG|2=INFO|3=WARN|4=ERROR|5=CRITICAL|6=FATAL")
	agents           = flag.Int("agents", 1000, "how many agents to simulate")
	metrics          = flag.Int("metrics", 10, "how many metrics per agent to simulate")
	period           = flag.Int("period", 10, "period in seconds between metric points")
	statsdAddr       = flag.String("statsd-addr", "localhost:8125", "statsd address")
	statsdType       = flag.String("statsd-type", "standard", "statsd type: standard or datadog")
)

func main() {
	flag.Parse()

	log.NewLogger(0, "console", fmt.Sprintf(`{"level": %d, "formatting":true}`, *logLevel))

	if *showVersion {
		fmt.Println("independent_agents")
		return
	}

	if *topic == "" {
		log.Fatal(4, "--topic is required")
	}

	if *carbonTCPAddr == "" && *kafkaMdmTCPAddr == "" && *nsqdTCPAddr == "" && !*stdoutOut {
		log.Fatal(4, "must use at least either carbon, kafka-mdm, nsq or stdout")
	}

	hostname, err := os.Hostname()
	if err != nil {
		log.Fatal(4, "failed to lookup hostname. %s", err)
	}

	// since we have so many small little outputs they would each be sending the same data which would be a bit crazy.
	stats, err := helper.New(false, *statsdAddr, *statsdType, "fake_metrics_agents", strings.Replace(hostname, ".", "_", -1))
	if err != nil {
		log.Fatal(4, "failed to initialize statsd. %s", err)
	}

	if *stdoutOut {
		std = stdout.New(stats)
	}

	run(*agents, *metrics, *period, *carbonTCPAddr, *kafkaMdmTCPAddr, *nsqdTCPAddr, stats)
	select {}
}

func run(agents, metrics, period int, carbonTCPAddr, kafkaMdmTCPAddr, nsqdTCPAddr string, stats met.Backend) {
	for i := 0; i < agents; i++ {
		go agent(i, metrics, period, carbonTCPAddr, kafkaMdmTCPAddr, nsqdTCPAddr, stats)
	}
}

func agent(id, metrics, period int, carbonTCPAddr, kafkaMdmTCPAddr, nsqdTCPAddr string, stats met.Backend) {
	// first sleep an arbitrary time between 0 and period, in ns
	sleep := time.Duration(rand.Intn(period * 1e9))
	time.Sleep(sleep)
	var outs []out.Out
	if carbonTCPAddr != "" {
		o, err := carbon.New(carbonTCPAddr, stats)
		if err != nil {
			log.Fatal(4, "agent %d failed to create carbon output. %s", id, err)
		}
		outs = append(outs, o)
	}

	if kafkaMdmTCPAddr != "" {
		o, err := kafkamdm.New(*topic, []string{kafkaMdmTCPAddr}, *kafkaCompression, stats)
		if err != nil {
			log.Fatal(4, "agent %d failed to create kafka-mdm output. %s", id, err)
		}
		outs = append(outs, o)
	}

	if nsqdTCPAddr != "" {
		o, err := nsq.New(*topic, nsqdTCPAddr, stats)
		if err != nil {
			log.Fatal(4, "agent %d failed to create NSQ output. %s", id, err)
		}
		outs = append(outs, o)
	}

	met := make([]*schema.MetricData, metrics)
	for i := 0; i < metrics; i++ {
		met[i] = &schema.MetricData{
			Name:       fmt.Sprintf("fake_metrics.independent_agent_%d.metric.%d", id, i),
			Metric:     "fake_metrics.independent_agent",
			OrgId:      1,
			Interval:   period,
			Value:      0,
			Unit:       "ms",
			TargetType: "gauge",
			Tags:       []string{"some_tag", "ok", fmt.Sprintf("agent:%d", id), fmt.Sprintf("met:%d", i)},
		}
		met[i].SetId()
	}

	do := func(t time.Time) {
		for i := 0; i < metrics; i++ {
			met[i].Time = t.Unix()
			met[i].Value = float64(id*metrics + i)
		}
		if std != nil {
			err := std.Flush(met)
			if err != nil {
				log.Error(0, err.Error())
			}
		}
		for _, out := range outs {
			err := out.Flush(met)
			if err != nil {
				log.Error(0, err.Error())
			}
		}
	}

	do(time.Now())
	tick := time.NewTicker(time.Duration(period) * time.Second)
	for t := range tick.C {
		do(t)
	}
}
