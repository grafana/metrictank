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
	"os"
	"os/signal"
	"syscall"
	"time"
	"sync"

	"github.com/codeskyblue/go-uuid"
	elastigo "github.com/mattbaird/elastigo/lib"
	"github.com/raintank/raintank-metric/schema"
)

var es *elastigo.Conn
var bulk *elastigo.BulkIndexer

const maxCons = 10
const retry = 60
const flushBulk = 60

type idxTrack struct {
	m sync.Mutex
	idxMap map[string]bool
	mapping map[string]map[string]bool
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

	bulk = es.NewBulkIndexerErrors(maxCons, retry)
	// start the indexer
	bulk.Start()

	esIdxTrack = new(idxTrack)
	esIdxTrack.idxMap = make(map[string]bool)
	esIdxTrack.mapping = make(map[string]map[string]bool)

	handleSignals()
	setErrorTicker()

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
	
	t := time.Now()
	y, m, d := t.Date()
	idxName := fmt.Sprintf("events-%d-%02d-%02d", y, m, d)
	if err := checkIdx(idxName, e); err != nil {
		return err
	}
	log.Printf("saving event to elasticsearch.")
	err := bulk.Index(idxName, e.EventType, e.Id, "", "", &t, e)
	log.Printf("event queued to bulk indexer")
	if err != nil {
		return err
	}

	return nil
}

func checkIdx(idxName string, e *schema.ProbeEvent) error {
	esIdxTrack.m.Lock()
	defer esIdxTrack.m.Lock()
	
	if !esIdxTrack.idxMap[idxName] {
		// make the index with the name and mapping
		exists, err := es.IndicesExists(idxName)
		if exists {
			if err != nil {
				return err
			}
		} else {
			_, err = es.CreateIndex(idxName)
			if err != nil {
				return err
			}
		}
		esIdxTrack.idxMap[idxName] = true
	}
	
	if _, ok := esIdxTrack.mapping[idxName]; !ok {
		esIdxTrack.mapping[idxName] = make(map[string]bool)
	}
	
	if !esIdxTrack.mapping[idxName][e.EventType] {
		var opt elastigo.MappingOptions
		err := es.PutMapping(idxName, e.EventType, e, opt)
		if err != nil {
			return err
		}
		esIdxTrack.mapping[idxName][e.EventType] = true
	}
	
	
	return nil
}

func handleSignals() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		for range c {
			log.Println("closing bulk indexer...")
			bulk.Stop()
		}
		os.Exit(0)
	}()
}

func setErrorTicker() {
	// log elasticsearch errors 
	go func() {
		for e := range bulk.ErrorChannel {
			log.Printf("elasticsearch bulk error: %s", e.Err.Error())
		}
	}()
}
