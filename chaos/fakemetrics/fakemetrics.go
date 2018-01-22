package fakemetrics

import (
	"fmt"
	"log"
	"time"

	"github.com/grafana/metrictank/chaos/fakemetrics/out/kafkamdm"
	"github.com/grafana/metrictank/clock"
	"github.com/raintank/met/helper"
	"gopkg.in/raintank/schema.v1"
)

const numPartitions = 12

var metrics []*schema.MetricData

func init() {
	for i := 0; i < numPartitions; i++ {
		name := fmt.Sprintf("some.id.of.a.metric.%d", i)
		m := &schema.MetricData{
			OrgId:    1,
			Name:     name,
			Interval: 1,
			Value:    1,
			Unit:     "s",
			Mtype:    "gauge",
		}
		m.SetId()
		metrics = append(metrics, m)
	}
}

func Kafka() {
	stats, _ := helper.New(false, "", "standard", "", "")
	out, err := kafkamdm.New("mdm", []string{"localhost:9092"}, "none", stats, "lastNum")
	if err != nil {
		log.Fatal(4, "failed to create kafka-mdm output. %s", err)
	}
	// advantage over regular ticker:
	// 1) no ticks dropped
	// 2) ticks come asap after the start of a new second, so we can measure better how long it took to get the data
	ticker := clock.AlignedTick(time.Second)

	for tick := range ticker {
		unix := tick.Unix()
		for i := range metrics {
			metrics[i].Time = unix
		}
		err := out.Flush(metrics)
		if err != nil {
			panic(fmt.Sprintf("failed to send data to kafka: %s", err))
		}
	}
}
