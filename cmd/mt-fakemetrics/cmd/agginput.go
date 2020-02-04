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
	"flag"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/spf13/cobra"
)

var agginputCmd = &cobra.Command{
	Use:   "agginput",
	Short: "A particular workload good to test performance of carbon-relay-ng aggregators",
	Run: func(cmd *cobra.Command, args []string) {
		initStats(true, "agginputs")
		agginput()
	},
}

func init() {
	rootCmd.AddCommand(agginputCmd)
}

func agginput() {
	flag.Parse()
	conn, err := net.Dial("tcp", carbonAddr)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	var metrics []string
	// 'addAgg sum core\.bidder\.pops\.(...)\.[A-Za-z0-9-]+\.([A-Za-z0-9_-]+)$             test.core.bidder.totals.$1.$2 30 60', // 100 total output series with 200 inputs
	// 'addAgg sum core\.bidder\.pops\.(...)\.[A-Za-z0-9-]+\.A\.([A-Za-z0-9_-]+)           test.core.bidder.totals.$1.A.$2 30 60', // 10 total output series with 200 inputs
	// 'addAgg sum core\.bidder\.pops\.(...)\.[A-Za-z0-9-]+\.B\.([A-Za-z0-9_-]+)           test.core.bidder.totals.$1.B.$2 30 60', // 10 total output series with 200 inputs
	// 'addAgg sum core\.bidder\.pops\.(...)\.[A-Za-z0-9-]+\.C\.(....?)\.([A-Za-z0-9_-]+)$ test.core.bidder.totals.$1.C.$2.$3 30 60', // 3k output series total output series with 200 inputs
	// we know that pop is a constant for a relay instance, so keep that constant. we chose 'nyc' here.
	// nodes that are parenthesis wrapped will result in a unique output key -> their amount of combos should be ~ total output series
	// nodes that are not wrapped get aggregagated together, we want about 200

	fmt.Println("building data..")
	pre := time.Now()

	var aggInputs []string
	for i := 0; i < 200; i++ {
		aggInputs = append(aggInputs, RandString(8))
	}

	// note that the random parts also have a predictable part to them, so that we can have a static dashboard config pointing to a single series even when it's random
	// like *-123 will point to 1 specific node.

	// 100 total, 200 inputs to each
	for i := 1; i <= 100; i++ {
		a := fmt.Sprintf("%s-%d", RandString(4), i)
		for _, input := range aggInputs {
			metrics = append(metrics, "core.bidder.pops.nyc."+input+"."+a)
		}
	}

	// 10 total, 200 inputs to each
	for i := 1; i <= 10; i++ {
		a := fmt.Sprintf("%s-%d", RandString(4), i)
		for _, input := range aggInputs {
			metrics = append(metrics, "core.bidder.pops.nyc."+input+".A."+a)
			metrics = append(metrics, "core.bidder.pops.nyc."+input+".B."+a)
		}
	}

	// 3k total, 200 inputs to each
	for i := 1; i <= 3500; i++ {
		a := fmt.Sprintf("%.4d", i)
		b := fmt.Sprintf("%s-%d", RandString(8), i)
		for _, input := range aggInputs {
			metrics = append(metrics, "core.bidder.pops.nyc."+input+".C."+a+"."+b)
		}
	}
	fmt.Println("building data done in", time.Since(pre))

	tick := time.NewTicker(30 * time.Second)
	for ts := range tick.C {
		fmt.Println("doing", ts)
		pre := time.Now()
		unix := ts.Unix()
		for _, metric := range metrics {
			_, err := fmt.Fprintf(conn, "%s 10.12 %d\n", metric, unix)
			if err != nil {
				fmt.Println(err)
			}
		}
		fmt.Println("done in", time.Since(pre))
	}
}
