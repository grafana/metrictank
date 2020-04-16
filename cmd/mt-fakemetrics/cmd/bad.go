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

	"github.com/grafana/metrictank/cmd/mt-fakemetrics/out"
	"github.com/grafana/metrictank/schema"
	"github.com/raintank/worldping-api/pkg/log"
	"github.com/spf13/cobra"
)

var flags struct {
	invalidTimestamp bool
	invalidInterval  bool
	invalidOrgID     bool
	invalidName      bool
	invalidMtype     bool
	invalidTags      bool
	outOfOrder       uint
	duplicate        bool
}

var badCmd = &cobra.Command{
	Use:   "bad",
	Short: "Sends out invalid/out-of-order/duplicate metric data",
	Run: func(cmd *cobra.Command, args []string) {
		initStats(true, "bad")
		out := getOutput()
		generateData(out)
	},
}

func init() {
	rootCmd.AddCommand(badCmd)
	badCmd.Flags().BoolVar(&flags.invalidTimestamp, "invalid-timestamp", false, "use an invalid timestamp")
	badCmd.Flags().BoolVar(&flags.invalidInterval, "invalid-interval", false, "use an invalid interval")
	badCmd.Flags().BoolVar(&flags.invalidOrgID, "invalid-orgid", false, "use an invalid orgId")
	badCmd.Flags().BoolVar(&flags.invalidName, "invalid-name", false, "use an invalid name")
	badCmd.Flags().BoolVar(&flags.invalidMtype, "invalid-mtype", false, "use an invalid mtype")
	badCmd.Flags().BoolVar(&flags.invalidTags, "invalid-tags", false, "use an invalid tag")
	badCmd.Flags().UintVar(&flags.outOfOrder, "out-of-order", 0, "send data periodically in an inverted order (optionally specify number of inverted data points per period)")
	badCmd.Flag("out-of-order").NoOptDefVal = "5"
	badCmd.Flags().BoolVar(&flags.duplicate, "duplicate", false, "send duplicate data")
}

func generateData(out out.Out) {
	md := &schema.MetricData{
		Name:     "some.id.of.a.metric.0",
		OrgId:    1,
		Interval: 1,
		Unit:     "s",
		Mtype:    "gauge",
		Tags:     nil,
	}

	if flags.invalidInterval {
		md.Interval = 0 // 0 or >= math.MaxInt32
	}

	if flags.invalidOrgID {
		md.OrgId = 0
	}

	if flags.invalidName {
		md.Name = ""
	}

	if flags.invalidMtype {
		md.Mtype = "invalid Mtype"
	}

	if flags.invalidTags {
		md.Tags = []string{"==invalid tags,#4561=="}
	}

	md.SetId()
	sl := []*schema.MetricData{md}

	tick := time.NewTicker(time.Second)
	for ts := range tick.C {
		timestamp := ts.Unix()
		if flags.invalidTimestamp {
			timestamp = 0 // 0 or >= math.MaxInt32
		} else if flags.outOfOrder > 0 {
			n := int64(flags.outOfOrder)
			// invert in time n data points with the following n data points
			if timestamp%(2*n) < n {
				timestamp -= n
			} else {
				timestamp += n
			}
		} else if flags.duplicate {
			if md.Time != 0 {
				timestamp = md.Time
			}
		}
		md.Time = timestamp
		md.Value = float64(2.0)
		err := out.Flush(sl)
		if err != nil {
			log.Error(0, err.Error())
		}
	}
}
