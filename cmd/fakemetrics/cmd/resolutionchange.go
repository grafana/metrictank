// Copyright © 2018 Grafana Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"fmt"

	"time"

	"github.com/grafana/metrictank/schema"
	"github.com/raintank/fakemetrics/out"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

// resolutionchangeCmd represents the resolutionchange command
var resolutionchangeCmd = &cobra.Command{
	Use:   "resolutionchange",
	Short: "Sends out metric with changing intervals, time range 24hours",
	Run: func(cmd *cobra.Command, args []string) {
		initStats(true, "resolutionchange")
		if mpo%10 != 0 {
			log.Fatal("mpo must divide by 10")
		}
		period = int(periodDur.Seconds())
		flush = int(flushDur.Nanoseconds() / 1000 / 1000)
		outs := getOutputs()
		if len(outs) == 0 {
			log.Fatal("need to define an output")
		}
		to := time.Now().Unix()
		from := to - 24*60*60

		var metrics []*schema.MetricData
		for i := 0; i < mpo; i++ {
			metrics = append(metrics, buildResChangeMetric("fakemetrics.reschange.%d", i, 1))
		}
		runResolutionChange(metrics, from, to, outs)
	},
}

func init() {
	rootCmd.AddCommand(resolutionchangeCmd)
	resolutionchangeCmd.Flags().IntVar(&mpo, "mpo", 10, "how many metrics per org to simulate (must be multiple of 10)")
}

func buildResChangeMetric(name string, i, org int) *schema.MetricData {
	out := &schema.MetricData{
		Name:     fmt.Sprintf(name, i),
		OrgId:    org,
		Interval: 1,
		Unit:     "ms",
		Mtype:    "gauge",
		Tags:     nil,
	}
	out.SetId()
	return out
}
func runResolutionChange(metrics []*schema.MetricData, from, to int64, outs []out.Out) {
	ts := from
	interval := int64(1)
	for ts <= to {
		if ts%3600 == 0 {
			fmt.Println("doing ts", ts)
		}
		// every 6 hours, increase resolution
		if ts%3600*6 == 0 {
			switch interval {
			case 1:
				interval = 10
			case 10:
				interval = 60
			case 60:
				interval = 300
			}
			fmt.Println("changing interval to", interval)
			for i := range metrics {
				metrics[i].Interval = int(interval)
				metrics[i].SetId()
			}
		}

		for i := range metrics {
			metrics[i].Time = ts
			metrics[i].Value = float64(ts % 10)
		}
		for _, out := range outs {
			err := out.Flush(metrics)
			if err != nil {
				log.Error(err.Error())
			}
		}
		ts += interval
	}
}
