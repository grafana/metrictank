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
	"net/http"
	_ "net/http/pprof"
	"os"
	"time"

	homedir "github.com/mitchellh/go-homedir"
	"github.com/raintank/fakemetrics/out"
	"github.com/raintank/met"
	"github.com/raintank/worldping-api/pkg/log"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var rootCmd = &cobra.Command{
	Use:   "fakemetrics",
	Short: "Generates fake metrics workload",
	PersistentPreRun: func(cmd *cobra.Command, args []string) {

		log.NewLogger(0, "console", fmt.Sprintf(`{"level": %d, "formatting":true}`, logLevel))

		if listenAddr != "" {
			go func() {
				log.Info("starting listener on %s", listenAddr)
				err := http.ListenAndServe(listenAddr, nil)
				if err != nil {
					log.Error(0, "%s", err)
				}
			}()
		}
	},
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

var (
	// config params used by >1 subcommands are listed here
	// config params specific to only 1 command, go in the file for that command
	cfgFile    string
	listenAddr string
	logLevel   int
	statsdAddr string
	statsdType string

	addTags             bool
	numUniqueTags       int
	customTags          []string
	numUniqueCustomTags int

	kafkaMdmAddr     string
	kafkaMdmTopic    string
	kafkaMdmV2       bool
	kafkaMdamAddr    string
	kafkaCompression string
	partitionScheme  string
	carbonAddr       string
	gnetAddr         string
	gnetKey          string
	stdoutOut        bool

	metricName string
	orgs       int
	mpo        int
	flushDur   time.Duration
	periodDur  time.Duration
	flush      int // in ms
	period     int // in s

	// global vars
	outs          []out.Out
	stats         met.Backend
	flushDuration met.Timer
)

func init() {
	cobra.OnInitialize(initConfig)

	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.fakemetrics.yaml)")
	rootCmd.PersistentFlags().StringVar(&listenAddr, "listen", ":6764", "http listener address for pprof.")
	rootCmd.PersistentFlags().IntVar(&logLevel, "log-level", 2, "log level. 0=TRACE|1=DEBUG|2=INFO|3=WARN|4=ERROR|5=CRITICAL|6=FATAL")
	rootCmd.PersistentFlags().StringVar(&statsdAddr, "statsd-addr", "", "statsd TCP address. e.g. 'localhost:8125'")
	rootCmd.PersistentFlags().StringVar(&statsdType, "statsd-type", "standard", "statsd type: standard or datadog")

	rootCmd.PersistentFlags().BoolVarP(&addTags, "add-tags", "t", false, "add the built-in tags to generated metrics (default false)")
	rootCmd.PersistentFlags().IntVar(&numUniqueTags, "num-unique-tags", 1, "a number between 0 and 10. when using add-tags this will add a unique number to some built-in tags")
	rootCmd.PersistentFlags().StringSliceVar(&customTags, "custom-tags", []string{}, "A list of comma separated tags (i.e. \"tag1=value1,tag2=value2\")(default empty) conflicts with add-tags")
	rootCmd.PersistentFlags().IntVar(&numUniqueCustomTags, "num-unique-custom-tags", 0, "a number between 0 and the length of custom-tags. when using custom-tags this will make the tags unique (default 0)")

	rootCmd.PersistentFlags().StringVar(&kafkaMdmAddr, "kafka-mdm-addr", "", "kafka TCP address for MetricData-Msgp messages. e.g. localhost:9092")
	rootCmd.PersistentFlags().StringVar(&kafkaMdmTopic, "kafka-mdm-topic", "mdm", "kafka topic for MetricData-Msgp messages")
	rootCmd.PersistentFlags().BoolVar(&kafkaMdmV2, "kafka-mdm-v2", true, "enable MetricPoint optimization (send MetricData first, then optimized MetricPoint payloads)")
	rootCmd.PersistentFlags().StringVar(&kafkaMdamAddr, "kafka-mdam-addr", "", "kafka TCP address for MetricDataArray-Msgp messages. e.g. localhost:9092")
	rootCmd.PersistentFlags().StringVar(&kafkaCompression, "kafka-comp", "snappy", "compression: none|gzip|snappy")
	rootCmd.PersistentFlags().StringVar(&partitionScheme, "partition-scheme", "bySeries", "method used for partitioning metrics (kafka-mdm-only). (byOrg|bySeries|bySeriesWithTags|bySeriesWithTagsFnv|lastNum)")
	rootCmd.PersistentFlags().StringVar(&carbonAddr, "carbon-addr", "", "carbon TCP address. e.g. localhost:2003")
	rootCmd.PersistentFlags().StringVar(&gnetAddr, "gnet-addr", "", "gnet address. e.g. http://localhost:8081")
	rootCmd.PersistentFlags().StringVar(&gnetKey, "gnet-key", "", "gnet api key")
	rootCmd.PersistentFlags().BoolVar(&stdoutOut, "stdout", false, "enable emitting metrics to stdout")
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	if cfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	} else {
		// Find home directory.
		home, err := homedir.Dir()
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		// Search config in home directory with name ".fakemetrics" (without extension).
		viper.AddConfigPath(home)
		viper.SetConfigName(".fakemetrics")
	}

	viper.AutomaticEnv() // read in environment variables that match

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err == nil {
		fmt.Println("Using config file:", viper.ConfigFileUsed())
	}
}
