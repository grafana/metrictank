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

package metricdef

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	elastigo "github.com/mattbaird/elastigo/lib"
	"github.com/raintank/worldping-api/pkg/log"
	"gopkg.in/raintank/schema.v1"
)

type DefsEs struct {
	index string
	*elastigo.Conn
	*elastigo.BulkIndexer
	cb ResultCallback
}

// cb can be nil now, as long as it's set by the time you start indexing.
func NewDefsEs(addr, user, pass, indexName string, cb ResultCallback) (*DefsEs, error) {
	parts := strings.Split(addr, ":")
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid tcp addr %q", addr)
	}

	d := &DefsEs{
		indexName,
		elastigo.NewConn(),
		nil,
		cb,
	}

	d.Conn.Domain = parts[0]
	d.Conn.Port = parts[1]
	if user != "" && pass != "" {
		d.Conn.Username = user
		d.Conn.Password = pass
	}
	if exists, err := d.ExistsIndex(indexName, "", nil); err != nil && err.Error() != "record not found" {
		return nil, err
	} else {
		if !exists {
			log.Info("ES: initializing %s Index with mapping", indexName)
			//lets apply the mapping.
			metricMapping := `{
				"mappings": {
		            "_default_": {
		                "dynamic_templates": [
		                    {
		                        "strings": {
		                            "mapping": {
		                                "index": "not_analyzed",
		                                "type": "string"
		                            },
		                            "match_mapping_type": "string"
		                        }
		                    }
		                ],
		                "_all": {
		                    "enabled": false
		                },
		                "properties": {}
		            },
		            "metric_index": {
		                "dynamic_templates": [
		                    {
		                        "strings": {
		                            "mapping": {
		                                "index": "not_analyzed",
		                                "type": "string"
		                            },
		                            "match_mapping_type": "string"
		                        }
		                    }
		                ],
		                "_all": {
		                    "enabled": false
		                },
		                "_timestamp": {
		                    "enabled": false
		                },
		                "properties": {
		                    "id": {
		                        "type": "string",
		                        "index": "not_analyzed"
		                    },
		                    "interval": {
		                        "type": "long"
		                    },
		                    "lastUpdate": {
		                        "type": "long"
		                    },
		                    "metric": {
		                        "type": "string",
		                        "index": "not_analyzed"
		                    },
		                    "name": {
		                        "type": "string",
		                        "index": "not_analyzed"
		                    },
		                    "node_count": {
		                        "type": "long"
		                    },
		                    "org_id": {
		                        "type": "long"
		                    },
		                    "tags": {
		                        "type": "string",
		                        "index": "not_analyzed"
		                    },
		                    "mtype": {
		                        "type": "string",
		                        "index": "not_analyzed"
		                    },
		                    "unit": {
		                        "type": "string",
		                        "index": "not_analyzed"
		                    }
		                }
					}
				}
			}`

			_, err = d.DoCommand("PUT", fmt.Sprintf("/%s", indexName), nil, metricMapping)
			if err != nil {
				return nil, err
			}
		}
	}

	//TODO:(awoods) make the following tuneable
	d.BulkIndexer = d.NewBulkIndexer(20)
	//dont retry sends.
	d.BulkIndexer.RetryForSeconds = 0
	// index at most 10k docs per request.
	d.BulkIndexer.BulkMaxDocs = 10000
	//flush at least every 10seconds.
	d.BulkIndexer.BufferDelayMax = time.Second * 10
	d.BulkIndexer.Refresh = true
	d.BulkIndexer.Sender = d.getBulkSend()

	d.BulkIndexer.Start()
	return d, nil
}

func (d *DefsEs) SetAsyncResultCallback(fn ResultCallback) {
	d.cb = fn
}

func (d *DefsEs) getBulkSend() func(buf *bytes.Buffer) error {
	return func(buf *bytes.Buffer) error {

		log.Debug("ES: sending defs batch")
		body, err := d.DoCommand("POST", fmt.Sprintf("/_bulk?refresh=%t", d.BulkIndexer.Refresh), nil, buf)

		// If something goes wrong at this stage, return an error and bulkIndexer will retry.
		if err != nil {
			log.Error(3, "ES: failed to send defs batch", err)
			return err
		}

		return d.processEsResponse(body)
	}
}

type responseStruct struct {
	Took   int64                    `json:"took"`
	Errors bool                     `json:"errors"`
	Items  []map[string]interface{} `json:"items"`
}

func (d *DefsEs) processEsResponse(body []byte) error {
	response := responseStruct{}

	// check for response errors, bulk insert will give 200 OK but then include errors in response
	err := json.Unmarshal(body, &response)
	if err != nil {
		// Something went *extremely* wrong trying to submit these items
		// to elasticsearch. return an error and bulkIndexer will retry.
		log.Error(3, "ES: bulkindex response parse failed: %q", err)
		return err
	}
	if response.Errors {
		log.Error(3, "ES: Bulk Insertion: some operations failed")
	} else {
		log.Debug("ES: Bulk Insertion: all operations succeeded")
	}
	for _, m := range response.Items {
		for _, v := range m {
			v := v.(map[string]interface{})
			id := v["_id"].(string)
			if errStr, ok := v["error"].(string); ok {
				d.cb(id, false)
				log.Debug("ES: %s failed: %s", id, errStr)
			} else if errMap, ok := v["error"].(map[string]interface{}); ok {
				d.cb(id, false)
				log.Debug("ES: %s failed: %s: %q", id, errMap["type"].(string), errMap["reason"].(string))
			} else {
				d.cb(id, true)
				log.Debug("ES: completed %s successfully.", id)
			}
		}
	}
	return nil
}

// if scroll_id specified, will resume that scroll session.
// returns scroll_id if there's any more metrics to be fetched.
func (d *DefsEs) GetMetrics(scroll_id string) ([]*schema.MetricDefinition, string, error) {
	// future optimiz: clear scroll when finished, tweak length of items, order by _doc
	// see https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-scroll.html
	defs := make([]*schema.MetricDefinition, 0)
	var err error
	var out elastigo.SearchResult
	if scroll_id == "" {
		out, err = d.Search(d.index, "metric_index", map[string]interface{}{"scroll": "1m", "size": 1000}, nil)
	} else {
		out, err = d.Scroll(map[string]interface{}{"scroll": "1m"}, scroll_id)
	}
	if err != nil {
		return defs, "", err
	}
	for _, h := range out.Hits.Hits {
		mdef, err := schema.MetricDefinitionFromJSON(*h.Source)
		if err != nil {
			return defs, "", err
		}
		defs = append(defs, mdef)
	}
	scroll_id = ""
	if out.Hits.Len() > 0 {
		scroll_id = out.ScrollId
	}

	return defs, scroll_id, nil
}

func (d *DefsEs) IndexMetric(m *schema.MetricDefinition) error {
	if err := m.Validate(); err != nil {
		return err
	}

	log.Debug("ES: indexing %s in elasticsearch", m.Id)
	err := d.BulkIndexer.Index(d.index, "metric_index", m.Id, "", "", nil, m)
	if err != nil {
		log.Error(3, "ES: failed to send payload to BulkApi indexer. %s", err)
		return err
	}

	return nil
}

func (d *DefsEs) GetMetricDefinition(id string) (*schema.MetricDefinition, bool, error) {
	if id == "" {
		panic("key cant be empty string.")
	}
	res, err := d.Get(d.index, "metric_index", id, nil)
	if err != nil {
		if err == elastigo.RecordNotFound {
			log.Debug("ES: %s not in ES. %s", id, err)
			return nil, false, nil
		} else {
			log.Error(3, "ES: elasticsearch query failed. %s", err)
			return nil, false, err
		}
	}
	def, err := schema.MetricDefinitionFromJSON(*res.Source)
	return def, true, err
}

func (d *DefsEs) Stop() {
	d.BulkIndexer.Stop()
}
