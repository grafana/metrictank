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

	"github.com/raintank/worldping-api/pkg/log"

	"github.com/raintank/fakemetrics/out"
	"github.com/raintank/fakemetrics/out/carbon"
	"github.com/raintank/fakemetrics/out/gnet"
	"github.com/raintank/fakemetrics/out/kafkamdam"
	"github.com/raintank/fakemetrics/out/kafkamdm"
	"github.com/raintank/fakemetrics/out/stdout"
)

func checkOutputs() {
	if carbonAddr == "" && gnetAddr == "" && kafkaMdmAddr == "" && kafkaMdamAddr == "" && !stdoutOut {
		log.Fatal(4, "must use at least either carbon, gnet, kafka-mdm, kafka-mdam or stdout")
	}
}

func getOutputs() []out.Out {
	var outs []out.Out

	if carbonAddr != "" {
		if orgs > 1 {
			log.Fatal(4, "can only simulate 1 org when using carbon output")
		}
		o, err := carbon.New(carbonAddr, stats)
		if err != nil {
			log.Fatal(4, "failed to create carbon output. %s", err)
		}
		outs = append(outs, o)
	}

	if gnetAddr != "" {
		if orgs > 1 {
			log.Fatal(4, "can only simulate 1 org when using gnet output")
		}
		if gnetKey == "" {
			log.Fatal(4, "to use gnet, a key must be specified")
		}
		o, err := gnet.New(gnetAddr, gnetKey, stats)
		if err != nil {
			log.Fatal(4, "failed to create gnet output. %s", err)
		}
		outs = append(outs, o)
	}

	if kafkaMdmAddr != "" {
		if kafkaMdmTopic == "" {
			log.Fatal(4, "kafka-mdm needs the topic to be set")
		}
		o, err := kafkamdm.New(kafkaMdmTopic, []string{kafkaMdmAddr}, kafkaCompression, 30*time.Second, stats, partitionScheme, kafkaMdmV2)
		if err != nil {
			log.Fatal(4, "failed to create kafka-mdm output. %s", err)
		}
		outs = append(outs, o)
	}

	if kafkaMdamAddr != "" {
		o, err := kafkamdam.New("mdam", []string{kafkaMdamAddr}, kafkaCompression, stats)
		if err != nil {
			log.Fatal(4, "failed to create kafka-mdam output. %s", err)
		}
		outs = append(outs, o)
	}

	if stdoutOut {
		outs = append(outs, stdout.New(stats))
	}

	return outs
}
