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
	"encoding/json"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/grafana/grafana/pkg/log"
	elastigo "github.com/mattbaird/elastigo/lib"
	"github.com/raintank/raintank-metric/schema"
	"gopkg.in/redis.v2"
)

func EnsureIndex(m *schema.MetricData) error {
	id := m.Id()
	def, err := GetMetricDefinition(id)
	if err != nil && err.Error() != "record not found" {
		return err
	}
	//if the definition does not exist, or is older then 10minutes. update it.
	if def == nil || def.LastUpdate < (time.Now().Unix()-600) {
		mdef := schema.MetricDefinitionFromMetricData(id, m)
		if err := Save(mdef); err != nil {
			return err
		}
	}
	return nil
}

var es *elastigo.Conn
var Indexer *elastigo.BulkIndexer
var IndexName = "metric"

// for the first 30minutes after startup, only
// write to ES 1% of the time. This allows us to
// slowly warmup a new or stale index.
var warmUpDuration = 1800
var warmUpPercent = 1
var startTime time.Time

func InitElasticsearch(addr, user, pass, indexName string) error {
	IndexName = indexName
	startTime = time.Now()
	rand.Seed(startTime.Unix())

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
	if exists, err := es.ExistsIndex(IndexName, "", nil); err != nil && err.Error() != "record not found" {
		return err
	} else {
		if !exists {
			log.Info("initializing %s Index with mapping", IndexName)
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
		                    "target_type": {
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

			_, err = es.DoCommand("PUT", fmt.Sprintf("/%s", IndexName), nil, metricMapping)
			if err != nil {
				return err
			}
		}
	}

	//TODO:(awoods) make the following tuneable
	Indexer = es.NewBulkIndexer(20)
	//dont retry sends.
	Indexer.RetryForSeconds = 0
	// index at most 10k docs per request.
	Indexer.BulkMaxDocs = 10000
	//flush at least every 10seconds.
	Indexer.BufferDelayMax = time.Second * 10
	Indexer.Refresh = true

	Indexer.Start()
	return nil
}

var rs *redis.Client

func InitRedis(addr string, db int, pass string) error {
	opts := &redis.Options{}
	opts.Network = "tcp"
	opts.Addr = addr
	if pass != "" {
		opts.Password = pass
	}
	opts.DB = int64(db)
	rs = redis.NewClient(opts)

	return nil
}

func Save(m *schema.MetricDefinition) error {
	if err := m.Validate(); err != nil {
		return err
	}
	// save in elasticsearch
	return indexMetric(m)
}

func indexMetric(m *schema.MetricDefinition) error {
	log.Debug("indexing %s in redis", m.Id)
	metricStr, err := json.Marshal(m)
	if err != nil {
		return err
	}
	if rerr := rs.SetEx(m.Id, time.Duration(1800)*time.Second, string(metricStr)).Err(); err != nil {
		log.Error(3, "redis err. %s", rerr)
	}
	if time.Since(startTime) < (time.Duration(warmUpDuration) * time.Second) {
		// we are in our warmup period.
		if rand.Intn(100) > warmUpPercent {
			return nil
		}
	}
	log.Debug("indexing %s in elasticsearch", m.Id)
	err = Indexer.Index(IndexName, "metric_index", m.Id, "", "", nil, m)
	if err != nil {
		log.Error(3, "failed to send payload to BulkApi indexer. %s", err)
		return err
	}

	return nil
}

// TODO: differentiate between record not found and error, so we can bubble up
// the right thing towards the user.
func GetMetricDefinition(id string) (*schema.MetricDefinition, error) {
	// TODO: fetch from redis before checking elasticsearch
	if v, err := rs.Get(id).Result(); err != nil && err != redis.Nil {
		log.Error(3, "The redis client bombed: %s", err)
		return nil, err
	} else if err == nil {
		//fmt.Printf("json for %s found in redis\n", id)
		def, err := schema.MetricDefinitionFromJSON([]byte(v))
		if err != nil {
			return nil, err
		}
		return def, nil
	}
	if time.Since(startTime) < (time.Duration(warmUpDuration) * time.Second) {
		// we are in our warmup period.
		return nil, fmt.Errorf("record not found")
	}
	log.Debug("%s not in redis. checking elasticsearch.", id)
	res, err := es.Get(IndexName, "metric_index", id, nil)
	if err != nil {
		if err == elastigo.RecordNotFound {
			log.Debug("%s not in ES. %s", id, err)
		} else {
			log.Error(3, "elasticsearch query failed. %s", err)
		}
		return nil, err
	}
	//fmt.Printf("elasticsearch query returned %q\n", res.Source)
	//fmt.Printf("placing %s into redis\n", id)
	if rerr := rs.SetEx(id, time.Duration(300)*time.Second, string(*res.Source)).Err(); err != nil {
		log.Error(3, "redis err. %s", rerr)
	}

	def, err := schema.MetricDefinitionFromJSON(*res.Source)
	if err != nil {
		return nil, err
	}

	return def, nil
}
