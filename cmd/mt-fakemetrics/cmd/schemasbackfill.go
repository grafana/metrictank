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
	"strings"
	"sync"

	"time"

	"github.com/grafana/metrictank/conf"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var mpr int
var ignore string
var schemasFile = "/etc/metrictank/storage-schemas.conf"

func init() {
	rootCmd.AddCommand(schemasbackfillCmd)
	schemasbackfillCmd.Flags().IntVar(&mpr, "mpr", 10, "how many metrics so simulate per rule")
	schemasbackfillCmd.Flags().StringVar(&schemasFile, "schemas-file", "/etc/metrictank/storage-schemas.conf", "path to storage-schemas.conf file")
	schemasbackfillCmd.Flags().StringVar(&ignore, "ignore", "default", "comma separated list of section names to exclude")
	schemasbackfillCmd.Flags().DurationVar(&offset, "offset", 0, "offset duration expression. (how far back in time to start. e.g. 1month, 6h, etc). must be a multiple of 1s")
	schemasbackfillCmd.Flags().IntVar(&speedup, "speedup", 1, "for each advancement of real time, how many advancements of fake data to simulate")
	schemasbackfillCmd.Flags().DurationVar(&flushDur, "flush", time.Second, "how often to flush metrics")
	schemasbackfillCmd.Flags().DurationVar(&periodDur, "period", time.Second, "period between metric points (must be a multiple of 1s)")
}

// schemasbackfillCmd represents the schemasbackfill command
var schemasbackfillCmd = &cobra.Command{
	Use:   "schemasbackfill",
	Short: "backfills a sends a set of metrics for each encountered storage-schemas.conf rule. Note: patterns must be a static string + wildcard at the end (e.g. foo.bar.*)!",
	Run: func(cmd *cobra.Command, args []string) {
		schemas, err := conf.ReadSchemas(schemasFile)
		if err != nil {
			log.Fatalf("can't read schemas file %q: %s", schemasFile, err.Error())
		}
		schemasList, _ := schemas.ListRaw()
		wg := &sync.WaitGroup{}
		initStats(true, "schemasbackfill")
		period = int(periodDur.Seconds())
		flush = int(flushDur.Nanoseconds() / 1000 / 1000)
		outs := getOutputs()
		if len(outs) == 0 {
			log.Fatal("need to define an output")
		}
		ignoreList := strings.Split(ignore, ",")

		wg.Add(len(schemasList))
		for _, schema := range schemasList {
			if in(schema.Name, ignoreList) {
				continue
			}
			name := schema.Pattern.String()
			if strings.HasSuffix(name, ".*") {
				name = name[:len(name)-2]
			}
			if strings.HasSuffix(name, "^") {
				name = name[1:]
			}
			// probably the default catchall was matched
			if name == "" {
				name = "default"
			}
			go func(name string, period int) {
				dataFeed(outs, 1, mpr, period, flush, int(offset.Seconds()), speedup, true, SimpleBuilder{name})
				wg.Done()
			}(name, schema.Retentions.Rets[0].SecondsPerPoint)
		}
		wg.Wait()
	},
}

func in(s string, values []string) bool {
	for _, val := range values {
		if s == val {
			return true
		}
	}
	return false
}
