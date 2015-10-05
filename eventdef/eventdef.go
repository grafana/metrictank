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
	"net"
	"strings"
	"time"
	"sync"

	"github.com/codeskyblue/go-uuid"
	elastigo "github.com/mattbaird/elastigo/lib"
	"github.com/raintank/raintank-metric/schema"
)

var es *elastigo.Conn

type idxTrack struct {
	m sync.Mutex
	idxMap map[string]bool
}
var esIdxTrack *idxTrack

func InitElasticsearch(addr, user, pass string) error {
	es = elastigo.NewConn()
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return err
	}
	es.Domain = host
	es.Port = port
	if user != "" && pass != "" {
		es.Username = user
		es.Password = pass
	}
	esIdxTrack = new(idxTrack)
	esIdxTrack.idxMap = make(map[string]bool)

	return nil
}

func Save(e *schema.ProbeEvent) error {
	if e.Id == "" {
		// per http://blog.mikemccandless.com/2014/05/choosing-fast-unique-identifier-uuid.html,
		// using V1 UUIDs is much faster than v4 like we were using
		u := uuid.NewUUID()
		e.Id = u.String()
	}
	if e.Timestamp == 0 {
		// looks like this expects timestamps in milliseconds
		e.Timestamp = time.Now().UnixNano() / int64(time.Millisecond)
	}
	if err := e.Validate(); err != nil {
		return err
	}
	
	y, m, d := time.Now().Date()
	idxName := fmt.Sprintf("events-%d-%02d-%02d", y, m, d)
	if err := checkIdx(idxName); err != nil {
		return err
	}
	log.Printf("saving event to elasticsearch.")
	resp, err := es.Index(idxName, e.EventType, e.Id, nil, e)
	log.Printf("elasticsearch response: %v", resp)
	if err != nil {
		return err
	}

	return nil
}

func checkIdx(idxName string) error {
	esIdxTrack.m.Lock()
	defer esIdxTrack.m.Lock()
	if !esIdxTrack.idxName[idxName] {
		// make the index with the name and mapping
		exists, err := es.IncidesExists(idxName)
		if exists {
			if err == nil {
				esIdxTrack.idxName[idxName] = true
				return nil
			} else {
				return err
			}
		}
		// TODO: use CreateIndexWithSettings
		_, err = es.CreateIndex(idxName)
		if err != nil {
			return err
		}
		esIdxTrack.idxName[idxName] = true
	}
	return nil
}
