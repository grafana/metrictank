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
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net"
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

type bulkSender struct {
	m sync.RWMutex
	conn *elastigo.Conn
	queued map[string]chan *BulkSaveStatus
	bulkIndexer *elastigo.BulkIndexer
	numErrors uint64
}

type BulkSaveStatus struct {
	Id string
	Ok bool
	Requeue bool
}

var bSender *bulkSender

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

	bSender = new(bulkSender)
	bSender.conn = es

	bulk = es.NewBulkIndexerErrors(maxCons, retry)
	bSender.bulkIndexer = bulk
	// start the indexer
	bulk.Start()

	setErrorTicker()

	return nil
}

func Save(e *schema.ProbeEvent, status chan *BulkSaveStatus) error {
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
	log.Printf("saving event to elasticsearch.")
	bSender.saveQueued(e.Id, status)
	err := bulk.Index(idxName, e.EventType, e.Id, "", "", &t, e)
	log.Printf("event queued to bulk indexer")
	if err != nil {
		return err
	}

	return nil
}

func (b *bulkSender) bulkSend(buf *bytes.Buffer) error {
	b.m.Lock()
	defer b.m.Unlock()
	// lovingly adaped from the elastigo bulk indexer Send function
	type responseStruct struct {
		Took   int64                    `json:"took"`
		Errors bool                     `json:"errors"`
		Items  []map[string]interface{} `json:"items"`
	}

	response := responseStruct{}

	body, err := b.conn.DoCommand("POST", fmt.Sprintf("/_bulk?refresh=%t", b.bulkIndexer.Refresh), nil, buf)

	if err != nil {
		b.numErrors += 1
		return err
	}
	// check for response errors, bulk insert will give 200 OK but then include errors in response
	jsonErr := json.Unmarshal(body, &response)
	var errSend error
	if jsonErr == nil {
		if response.Errors {
			b.numErrors += uint64(len(response.Items))
			errSend = fmt.Errorf("Bulk Insertion Error. Failed item count [%d]", len(response.Items))
			// But what is in response.Items?
			for k, v := range response.Items {
				log.Printf("Contents of error item %s: %T %q", k, v, v)
			}
		}
		queued := b.queued
		b.queued = make(map[string]chan *BulkSaveStatus)
		// ack/requeue in a goroutine and let the sender move on
		go func(q map[string]chan *BulkSaveStatus, items []map[string]interface{}){
			// This will be easier once we know what response.Items
			// looks like. For now, ack everything
			for k, v := range q {
				stat := new(BulkSaveStatus)
				stat.Id = k
				stat.Ok = true
				v <- stat
			}
		}(queued, response.Items)
	} else {
		return fmt.Errorf("something went terribly wrong bulk loading: %s", jsonErr.Error())
	}
	if errSend != nil {
		return errSend
	}
	return nil
}

func (b *bulkSender) saveQueued(id string, status chan *BulkSaveStatus) {
	b.m.Lock()
	defer b.m.Unlock()
	b.queued[id] = status
}

func StopBulkIndexer() {
	log.Println("closing bulk indexer...")
	bulk.Stop()
}

func setErrorTicker() {
	// log elasticsearch errors 
	go func() {
		for e := range bulk.ErrorChannel {
			log.Printf("elasticsearch bulk error: %s", e.Err.Error())
		}
	}()
}
