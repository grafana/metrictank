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
	"net"
	"time"

	"github.com/codeskyblue/go-uuid"
	"github.com/raintank/worldping-api/pkg/log"
	elastigo "github.com/mattbaird/elastigo/lib"
	"github.com/raintank/raintank-metric/schema"
)

var (
	es          *elastigo.Conn
	bulk        *elastigo.BulkIndexer
	writeStatus chan *BulkSaveStatus
)

const maxCons = 10
const retry = 60
const flushBulk = 60

type BulkSaveStatus struct {
	Id string
	Ok bool
}

func InitElasticsearch(addr, user, pass string, w chan *BulkSaveStatus, bulkMaxDocs int) error {
	writeStatus = w
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

	// ensure that our index templates exist
	tmpl := `{
		"template" : "events-*",
		"mappings" : {
			"_default_" : {
				"_all" : {
					"enabled" : false
				},
				"dynamic_templates": [
					{
						"strings": {
							"mapping": {
								"index": "not_analyzed",
								"type": "string"
							},
							"match_mapping_type": "string",
							"umatch": "message",
							"path_unmatch": "tags.*"
						}
					},
					{
						"message": {
							"mapping": {
								"type": "string",
								"norms": {
									"enabled": false
								},
								"index_options": "docs"
							},
							"match": "message"
						}
					},
					{
						"tag_values": {
							"mapping": {
								"type": "string",
								"index": "not_analyzed",
								"norms": {
									"enabled": false
								},
								"index_options": "docs"
							},
							"path_match": "tags.*"
						}
					}
				]
			}
		}
	}`
	_, err = es.DoCommand("PUT", fmt.Sprintf("/_template/events"), nil, tmpl)
	if err != nil {
		return err
	}
	initBulkIndexer(bulkSend, bulkMaxDocs)
	setErrorTicker()
	return nil
}

func initBulkIndexer(bulkSend func(*bytes.Buffer) error, bulkMaxDocs int) {
	// Now create the actual bulk indexer and assign the custom bulkSend
	// function to it as its sending function (so we have more control over
	// how it handles errors)
	bulk = es.NewBulkIndexerErrors(maxCons, retry)
	bulk.Sender = bulkSend
	bulk.BulkMaxDocs = bulkMaxDocs
	// start the indexer
	bulk.Start()
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

	// Craft the elasticsearch index name from the event's timestamp
	t := time.Unix(e.Timestamp/1000, 0)
	y, m, d := t.Date()
	idxName := fmt.Sprintf("events-%d-%02d-%02d", y, m, d)

	log.Debug("saving event to elasticsearch.")
	// Add the event to the bulk indexer's queue
	err := bulk.Index(idxName, e.EventType, e.Id, "", "", &t, e)
	log.Debug("event queued to bulk indexer")
	return err
}

// lovingly adaped from the elastigo bulk indexer Send function
type responseStruct struct {
	Took   int64                    `json:"took"`
	Errors bool                     `json:"errors"`
	Items  []map[string]interface{} `json:"items"`
}

func bulkSend(buf *bytes.Buffer) error {

	log.Debug("sending batch of events to Elastic")
	body, err := es.DoCommand("POST", fmt.Sprintf("/_bulk?refresh=%t", bulk.Refresh), nil, buf)

	// If something goes wrong at this stage, return an error and bulkIndexer will retry.
	if err != nil {
		log.Error(3, "failed to send batch of events to ES.", err)
		return err
	}

	return processEsResponse(body)
}

func processEsResponse(body []byte) error {
	response := responseStruct{}

	// check for response errors, bulk insert will give 200 OK but then include errors in response
	jsonErr := json.Unmarshal(body, &response)
	log.Debug(string(body))
	if jsonErr == nil {
		if response.Errors {
			log.Error(3, "Bulk Insertion Error. Failed item count [%d]", len(response.Items))
		}
		if len(response.Items) > 0 {
			for _, m := range response.Items {
				for _, v := range m {
					stat := &BulkSaveStatus{
						Id: v.(map[string]interface{})["_id"].(string),
						Ok: true,
					}
					if errStr, ok := v.(map[string]interface{})["error"]; ok {
						stat.Ok = false
						log.Debug("%s failed. %s", stat.Id, errStr)
					} else {
						log.Debug("completed %s successfully.", stat.Id)
					}
					writeStatus <- stat
				}
			}
		}
	} else {
		// Something went *extremely* wrong trying to submit these items
		// to elasticsearch. return an error and bulkIndexer will retry.
		log.Error(3, "something went terribly wrong bulk loading: %s", jsonErr)
		return jsonErr
	}
	return nil
}

// Stop the bulk indexer when we're shutting down
func StopBulkIndexer() {
	log.Info("closing bulk indexer...")
	bulk.Stop()
}

func setErrorTicker() {
	// log elasticsearch errors
	go func() {
		for e := range bulk.ErrorChannel {
			log.Warn("elasticsearch bulk error: %s", e.Err.Error())
		}
	}()
}
