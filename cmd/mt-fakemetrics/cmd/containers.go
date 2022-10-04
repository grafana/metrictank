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
	"strconv"
	"strings"

	"github.com/spf13/cobra"

	"time"

	"github.com/grafana/metrictank/cmd/mt-fakemetrics/out"
	"github.com/grafana/metrictank/cmd/mt-fakemetrics/policy"
	"github.com/grafana/metrictank/pkg/clock"
	"github.com/grafana/metrictank/pkg/schema"
	log "github.com/sirupsen/logrus"
)

var containersCmd = &cobra.Command{
	Use:   "containers",
	Short: "Mimic a set of containers - with churn - whose stats get reported at the same time",
	Run: func(cmd *cobra.Command, args []string) {
		checkOutputs()
		period = int(periodDur.Seconds())

		// since we have so many small little outputs they would each be sending the same data which would be a bit crazy.
		initStats(false, "containers")
		churnSplit := strings.Split(churn, ":")
		if len(churnSplit) != 3 {
			log.Fatalf("churnspec: <cycle time>:<cycle time noise>:<pct of containers to cycle>")
		}
		cycleDur, err := time.ParseDuration(churnSplit[0])
		if err != nil {
			log.Fatalf("churnspec: <cycle time>:<cycle time noise>:<pct of containers to cycle>")
		}
		cycleJitter, err := time.ParseDuration(churnSplit[1])
		if err != nil {
			log.Fatalf("churnspec: <cycle time>:<cycle time noise>:<pct of containers to cycle>")
		}
		cyclePct, err := strconv.Atoi(churnSplit[2])
		if err != nil {
			log.Fatalf("churnspec: <cycle time>:<cycle time noise>:<pct of containers to cycle>")
		}
		if cyclePct < 0 || cyclePct > 100 {
			log.Fatalf("churnspec: <cycle time>:<cycle time noise>:<pct of containers to cycle>")
		}

		rand.Seed(time.Now().UnixNano())

		for i := 0; i < numContainers; i++ {
			var key [8]byte
			rand.Read(key[:])
			containers = append(containers, key)
		}

		vp, err := policy.ParseValuePolicy(valuePolicy)
		if err != nil {
			panic(err)
		}
		monitorContainers(cycleDur, cycleJitter, cyclePct, vp)
	},
}

var (
	containers            [][8]byte
	numContainers         int
	numContainersPerBatch int
	metricsPerContainer   int
	churn                 string
)

func init() {
	rootCmd.AddCommand(containersCmd)
	containersCmd.Flags().IntVar(&numContainers, "containers", 1000, "how many containers to simulate")
	containersCmd.Flags().IntVar(&numContainersPerBatch, "batch", 10, "how many containers's metrics should go into each batch")
	containersCmd.Flags().IntVar(&metricsPerContainer, "metrics", 10, "how many metrics per container to simulate")
	containersCmd.Flags().DurationVar(&periodDur, "period", 10*time.Second, "period between metric points (must be a multiple of 1s)")
	containersCmd.Flags().StringVar(&churn, "churn", "4h:0h:50", "churn spec: <cycle time>:<cycle time noise>:<pct of containers to cycle>")
	containersCmd.Flags().StringVar(&valuePolicy, "value-policy", "", "a value policy (i.e. \"single:1\" \"multiple:1,2,3,4,5\" \"timestamp\" \"daily-sine:<peak>,<offset>,<stdev>\")")

}

// containerChurn keeps pct of the existing containers (random selection) and replaces the other ones with new ones
// during this process, containers also gets shuffled
func containerChurn(pct int) {
	rand.Shuffle(len(containers), func(i, j int) {
		containers[i], containers[j] = containers[j], containers[i]
	})
	for i := pct * len(containers) / 100; i < len(containers); i++ {
		var key [8]byte
		rand.Read(key[:])
		containers[i] = key
	}
}

func containerFlush(now time.Time, vp policy.ValuePolicy, out out.Out) {
	var batch []*schema.MetricData
	ts := now.Unix()
	var numContainers int
	for _, id := range containers {
		for i := 0; i < metricsPerContainer; i++ {
			met := &schema.MetricData{
				Name:     fmt.Sprintf("mt-fakemetrics.container.%x.metric.%d", id, i),
				OrgId:    1,
				Interval: period,
				Value:    vp.Value(ts),
				Unit:     "ms",
				Mtype:    "gauge",
				Time:     ts,
			}
			met.SetId()
			batch = append(batch, met)
		}
		numContainers++
		if numContainers == numContainersPerBatch {
			err := out.Flush(batch)
			if err != nil {
				log.Error(0, err.Error())
			}
			numContainers = 0
			batch = batch[:0]
		}
	}
	if numContainers > 0 {
		err := out.Flush(batch)
		if err != nil {
			log.Error(0, err.Error())
		}
	}
}

func monitorContainers(cycleDur, cycleJitter time.Duration, cyclePct int, vp policy.ValuePolicy) {
	out := getOutput()
	tickFlush := clock.AlignedTickLossless(periodDur)
	tickChurn := clock.NewRandomTicker(cycleDur, cycleJitter, true)
	for {
		select {
		case t := <-tickFlush:
			containerFlush(t, vp, out)
		case <-tickChurn.C:
			containerChurn(cyclePct)
		}
	}
}
