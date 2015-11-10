/*
 * Copyright (c) 2015, Raintank Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package eventdef

// identifier of message format
const (
	msgFormatJson = iota
)

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/raintank/raintank-metric/schema"
	"github.com/nsqio/go-nsq"
	"log"
	"os"
	"testing"
	"time"
)

var testProducer *nsq.Producer

func init() {
	nsqAddr := "localhost:4150"
	cfg:= nsq.NewConfig()
	cfg.UserAgent = "probe-ctrl-test"
	var nsqErr error
	testProducer, err = nsq.NewProducer(nsqAddr, cfg)
	if nsqErr != nil {
		panic(err)
	}
	
	addr := "localhost:9200"
	err := InitElasticsearch(addr, "", "")
	if err != nil {
		panic(err)
	}
	go func(){
		ticker := time.NewTicker(time.Second)
		for range ticker.C {
			publisher()
		}
	}
}

func publisher() {
	e := new(schema.ProbeEvent)
	e.EventType := "boom"
	e.OrgId = 1
	e.Severity = "ERROR"
	e.Source = "space"
	e.Timestamp = time.Now().UnixNano() / int64(time.Millisecond)
	version := uint8(msgFormatJson)

	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, version)
	if err != nil {
		log.Fatal(0, "binary.Write failed: %s", err.Error())
	}
	id := time.Now().UnixNano()
	binary.Write(buf, binary.BigEndian, id)
	if err != nil {
		log.Fatal(0, "binary.Write failed: %s", err.Error())
	}
	msg, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("Failed to marshal event payload: %s", err)
	}
	_, err = buf.Write(msg)
	if err != nil {
		log.Fatal(0, "buf.Write failed: %s", err.Error())
	}
	err = testProducer.Publish("probe_events", buf.Bytes())
	if err != nil {
		panic(fmt.Errorf("can't publish to nsqd: %s", err))
	}
	log.Info("event published to NSQ %d", id)
}

// let's try pulling in an event and saving it, eh?

func TestSave(t *testing.T) {

}
