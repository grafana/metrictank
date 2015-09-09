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
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/codeskyblue/go-uuid"
	elastigo "github.com/mattbaird/elastigo/lib"
	"github.com/raintank/raintank-metric/schema"
)

var es *elastigo.Conn

func InitElasticsearch(addr, user, pass string) error {
	es = elastigo.NewConn()
	parts := strings.Split(addr, ":")
	if len(parts) != 2 {
		return fmt.Errorf("invalid tcp addr %q", addr)
	}
	es.Domain = parts[0]
	es.Port = parts[1]
	if user != "" && pass != "" {
		es.Username = user
		es.Password = pass
	}

	return nil
}

func Save(e *schema.ProbeEvent) error {
	if e.Id == "" {
		u := uuid.NewRandom()
		e.Id = u.String()
	}
	if e.Timestamp == 0 {
		// looks like this expects timestamps in milliseconds
		e.Timestamp = time.Now().UnixNano() / int64(time.Millisecond)
	}
	if err := e.Validate(); err != nil {
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
