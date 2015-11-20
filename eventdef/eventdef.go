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
	"sync"
	"time"

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
	m           sync.RWMutex
	conn        *elastigo.Conn
	queued      map[string]chan *BulkSaveStatus
	bulkIndexer *elastigo.BulkIndexer
	numErrors   uint64
}

type BulkSaveStatus struct {
	Id      string
	Ok      bool
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

	// Create the custom bulk sender, its map of queued event setatuses, and
	// add the elasticsearhc connection
	bSender = new(bulkSender)
	bSender.queued = make(map[string]chan *BulkSaveStatus)
	bSender.conn = es

	// Now create the actual bulk indexer and assign the custom bulkSend
	// function to it as its sending function (so we have more control over
	// how it handles errors)
	bulk = es.NewBulkIndexerErrors(maxCons, retry)
	bulk.Sender = bSender.bulkSend
	// The custom bulk sender needs to have access to the parent bulk
	// indexer
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

	// Craft the elasticsearch index name from the event's timestamp
	t := time.Unix(e.Timestamp/1000, 0)
	y, m, d := t.Date()
	idxName := fmt.Sprintf("events-%d-%02d-%02d", y, m, d)

	// Add the event's status channel to the map of event statuses
	bSender.saveQueued(e.Id, status)

	log.Printf("saving event to elasticsearch.")
	// Add the event to the bulk indexer's queue
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

	// Take map of channels of event statuses to save, and assign a new one
	// to the bulk sender
	queued := b.queued
	b.queued = make(map[string]chan *BulkSaveStatus)

	body, err := b.conn.DoCommand("POST", fmt.Sprintf("/_bulk?refresh=%t", b.bulkIndexer.Refresh), nil, buf)

	// If something goes wrong at this stage, everything needs to be
	// requeued and submitted again. If it fails here it's because
	// elasticsearch itself isn't running or can't be reached.
	if err != nil {
		b.numErrors += 1
		go resubmitAll(queued)
		return err
	}
	// check for response errors, bulk insert will give 200 OK but then include errors in response
	jsonErr := json.Unmarshal(body, &response)
	var errSend error
	if jsonErr == nil {
		if response.Errors {
			b.numErrors += uint64(len(response.Items))
			errSend = fmt.Errorf("Bulk Insertion Error. Failed item count [%d]", len(response.Items))
		}
		// ack/requeue in a goroutine and let the sender move on
		go func(q map[string]chan *BulkSaveStatus, items []map[string]interface{}) {
			// If saving any items in the bulk save failed,
			// response.Items will be populated. However, successful
			// saves may be in there as well. Go through the items
			// if there are any and populate this map of bools to
			// indicate which items in response.Items are actual
			// failures. This is used below.
			errReqs := make(map[string]bool)
			if len(items) > 0 {
				for _, m := range items {
					for _, v := range m {
						if _, ok := v.(map[string]interface{})["error"]; ok {
							errReqs[v.(map[string]interface{})["_id"].(string)] = true
						}
					}
				}
			}
			// Go through our map of status channels. If the event
			// is present in the errReqs map of bools, it needs to
			// be requeued. Otherwise it saved successfully. In
			// either case, send the status into the channel for
			// the caller to acknowledge or requeue as needed.
			for k, v := range q {
				stat := new(BulkSaveStatus)
				stat.Id = k
				if errReqs[k] {
					stat.Requeue = true
				} else {
					stat.Ok = true
				}
				v <- stat
			}
		}(queued, response.Items)
	} else {
		// Something went *extremely* wrong trying to submit these items
		// to elasticsearch. Still, we ought to resubmit them.
		b.numErrors += 1
		go resubmitAll(queued)
		errSend = fmt.Errorf("something went terribly wrong bulk loading: %s", jsonErr.Error())
	}
	if errSend != nil {
		return errSend
	}
	return nil
}

func resubmitAll(q map[string]chan *BulkSaveStatus) {
	for k, v := range q {
		stat := new(BulkSaveStatus)
		stat.Id = k
		stat.Requeue = true
		v <- stat
	}
}

// Add the event's status channel to the map of status to save
func (b *bulkSender) saveQueued(id string, status chan *BulkSaveStatus) {
	b.m.Lock()
	defer b.m.Unlock()
	b.queued[id] = status
}

// Stop the bulk indexer when we're shutting down
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
