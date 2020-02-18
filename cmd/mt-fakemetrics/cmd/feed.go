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

	"github.com/spf13/cobra"
)

var feedCmd = &cobra.Command{
	Use:   "feed",
	Short: "Publishes a realtime feed of data",
	Run: func(cmd *cobra.Command, args []string) {
		initStats(true, "feed")
		period = int(periodDur.Seconds())
		flush = int(flushDur.Nanoseconds() / 1000 / 1000)
		outs := getOutputs()
		dataFeed(outs, orgs, mpo, period, flush, 0, 1, false, TaggedBuilder{metricName})

	},
}

func init() {
	rootCmd.AddCommand(feedCmd)
	feedCmd.Flags().StringVar(&metricName, "metricname", "some.id.of.a.metric", "the metric name to use")
	feedCmd.Flags().IntVar(&orgs, "orgs", 1, "how many orgs to simulate")
	feedCmd.Flags().IntVar(&mpo, "mpo", 100, "how many metrics per org to simulate")
	feedCmd.Flags().DurationVar(&flushDur, "flush", time.Second, "how often to flush metrics")
	feedCmd.Flags().DurationVar(&periodDur, "period", time.Second, "period between metric points (must be a multiple of 1s)")
}
