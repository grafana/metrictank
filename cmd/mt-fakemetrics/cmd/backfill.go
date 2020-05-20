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
	"time"

	"github.com/grafana/metrictank/cmd/mt-fakemetrics/policy"
	"github.com/spf13/cobra"
)

var backfillCmd = &cobra.Command{
	Use:   "backfill",
	Short: "backfills old data and stops when 'now' is reached",
	Run: func(cmd *cobra.Command, args []string) {
		initStats(true, "backfill")
		period = int(periodDur.Seconds())
		flush = int(flushDur.Nanoseconds() / 1000 / 1000)
		out := getOutput()

		vp, err := policy.ParseValuePolicy(valuePolicy)
		if err != nil {
			panic(err)
		}

		dataFeed(out, orgs, mpo, period, flush, int(offset.Seconds()), speedup, true, getBuilder(metricBuilder, metricName), vp)
	},
}

func init() {
	rootCmd.AddCommand(backfillCmd)
	backfillCmd.Flags().StringVar(&metricName, "metricname", "some.id.of.a.metric.%d", "the metric name to use")
	backfillCmd.Flags().StringVar(&metricBuilder, "metricbuilder", "simple", "the metric builder to use. (simple|tagged)")
	backfillCmd.Flags().DurationVar(&offset, "offset", 0, "offset duration expression. (how far back in time to start. e.g. 1month, 6h, etc). must be a multiple of 1s")
	backfillCmd.Flags().IntVar(&orgs, "orgs", 1, "how many orgs to simulate")
	backfillCmd.Flags().IntVar(&mpo, "mpo", 100, "how many metrics per org to simulate")
	backfillCmd.Flags().IntVar(&speedup, "speedup", 1, "for each advancement of real time, how many advancements of fake data to simulate")
	backfillCmd.Flags().DurationVar(&flushDur, "flush", time.Second, "how often to flush metrics")
	backfillCmd.Flags().DurationVar(&periodDur, "period", time.Second, "period between metric points (must be a multiple of 1s)")
	backfillCmd.Flags().StringVar(&valuePolicy, "value-policy", "", "a value policy (i.e. \"single:1\" \"multiple:1,2,3,4,5\" \"timestamp\" \"daily-sine:<peak>,<offset>,<stdev>\")")
}
