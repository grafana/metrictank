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

import (
	"encoding/json"
	"fmt"
	"github.com/codeskyblue/go-uuid"
	elastigo "github.com/mattbaird/elastigo/lib"
	"github.com/raintank/raintank-metric/setting"
	"log"
	"strconv"
	"strings"
	"time"
)

type EventDefinition struct {
	Id        string            `json:"id"`
	EventType string            `json:"event_type"`
	OrgId     int64             `json:"org_id"`
	Severity  string            `json:"severity"` // enum "INFO" "WARN" "ERROR" "OK"
	Source    string            `json:"source"`
	Timestamp int64             `json:"timestamp"`
	Message   string            `json:"message"`
	Tags      map[string]string `json:"tags"`
}

var es *elastigo.Conn

func InitElasticsearch() error {
	es = elastigo.NewConn()
	es.Domain = setting.Config.ElasticsearchDomain // needs to be configurable obviously
	es.Port = strconv.Itoa(setting.Config.ElasticsearchPort)
	if setting.Config.ElasticsearchUser != "" && setting.Config.ElasticsearchPasswd != "" {
		es.Username = setting.Config.ElasticsearchUser
		es.Password = setting.Config.ElasticsearchPasswd
	}

	return nil
}

func (e *EventDefinition) Save() error {
	if e.Id == "" {
		u := uuid.NewRandom()
		e.Id = u.String()
	}
	if e.Timestamp == 0 {
		// looks like this expects timestamps in milliseconds
		e.Timestamp = time.Now().UnixNano() / int64(time.Millisecond)
	}
	if err := e.validate(); err != nil {
		return err
	}
	log.Printf("saving event to elasticsearch.")
	resp, err := es.Index("events", e.EventType, e.Id, nil, e)
	log.Printf("elasticsearch response: %v", resp)
	if err != nil {
		return err
	}

	return nil
}

func (e *EventDefinition) validate() error {
	if e.EventType == "" || e.OrgId == 0 || e.Source == "" || e.Timestamp == 0 || e.Message == "" {
		err := fmt.Errorf("event definition not valid")
		return err
	}
	switch strings.ToLower(e.Severity) {
	case "info", "ok", "warn", "error", "warning", "critical":
		// nop
	default:
		err := fmt.Errorf("'%s' is not a valid severity level", e.Severity)
		return err
	}
	return nil
}

func EventFromJSON(b []byte) (*EventDefinition, error) {
	e := new(EventDefinition)
	if err := json.Unmarshal(b, &e); err != nil {
		return nil, err
	}
	return e, nil
}
