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
	"math/rand"

	"github.com/spf13/cobra"

	"time"

	"github.com/grafana/metrictank/schema"
	"github.com/raintank/worldping-api/pkg/log"
)

var agentsCmd = &cobra.Command{
	Use:   "agents",
	Short: "Mimic independent agents",
	Run: func(cmd *cobra.Command, args []string) {
		checkOutputs()
		period = int(periodDur.Seconds())

		// since we have so many small little outputs they would each be sending the same data which would be a bit crazy.
		initStats(false, "agents")

		for i := 0; i < agents; i++ {
			go agent(i)
		}
		select {}
	},
}

var (
	agents          int
	metricsPerAgent int
)

func init() {
	rootCmd.AddCommand(agentsCmd)
	agentsCmd.Flags().IntVar(&agents, "agents", 1000, "how many agents to simulate")
	agentsCmd.Flags().IntVar(&metricsPerAgent, "metrics", 10, "how many metrics per agent to simulate")
	agentsCmd.Flags().DurationVar(&periodDur, "period", 10*time.Second, "period between metric points (must be a multiple of 1s)")
}

func agent(id int) {
	// first sleep an arbitrary time between 0 and period
	sleep := time.Duration(rand.Intn(int(period)))
	time.Sleep(sleep)
	outs := getOutputs()

	met := make([]*schema.MetricData, metricsPerAgent)
	for i := 0; i < metricsPerAgent; i++ {
		met[i] = &schema.MetricData{
			Name:     fmt.Sprintf("fakemetrics.agent_%d.metric.%d", id, i),
			OrgId:    1,
			Interval: period,
			Value:    0,
			Unit:     "ms",
			Mtype:    "gauge",
			Tags:     []string{"some_tag", "ok", fmt.Sprintf("agent:%d", id), fmt.Sprintf("met:%d", i)},
		}
		met[i].SetId()
	}

	do := func(t time.Time) {
		for i := 0; i < metricsPerAgent; i++ {
			met[i].Time = t.Unix()
			met[i].Value = float64(id*metricsPerAgent + i)
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
