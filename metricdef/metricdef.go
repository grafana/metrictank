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
	"log"
	"strconv"
	"time"

	elastigo "github.com/mattbaird/elastigo/lib"
	"github.com/raintank/raintank-metric/schema"
	"github.com/raintank/raintank-metric/setting"
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

func InitElasticsearch() error {
	es = elastigo.NewConn()
	es.Domain = setting.Config.ElasticsearchDomain // needs to be configurable obviously
	es.Port = strconv.Itoa(setting.Config.ElasticsearchPort)
	if setting.Config.ElasticsearchUser != "" && setting.Config.ElasticsearchPasswd != "" {
		es.Username = setting.Config.ElasticsearchUser
		es.Password = setting.Config.ElasticsearchPasswd
	}
	if exists, err := es.ExistsIndex("metric", "metric_index", nil); err != nil && err.Error() != "record not found" {
		return err
	} else {
		if !exists {
			_, err = es.CreateIndex("metric")
			if err != nil {
				return err
			}
		}
		esopts := elastigo.MappingOptions{}

		err = es.PutMapping("metric", "metric_index", schema.MetricDefinition{}, esopts)
		if err != nil {
			return err
		}
	}
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

func InitRedis() error {
	opts := &redis.Options{}
	opts.Network = "tcp"
	opts.Addr = setting.Config.RedisAddr
	if setting.Config.RedisPasswd != "" {
		opts.Password = setting.Config.RedisPasswd
	}
	opts.DB = setting.Config.RedisDB
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
	log.Printf("indexing %s in redis\n", m.Id)
	metricStr, err := json.Marshal(m)
	if err != nil {
		return err
	}
	if rerr := rs.SetEx(m.Id, time.Duration(300)*time.Second, string(metricStr)).Err(); err != nil {
		fmt.Printf("redis err: %s", rerr.Error())
	}

	log.Printf("indexing %s in elasticsearch\n", m.Id)
	err = Indexer.Index("metric", "metric_index", m.Id, "", "", nil, m)
	if err != nil {
		log.Printf("failed to send payload to BulkApi indexer.")
		return err
	}

	return nil
}

func GetMetricDefinition(id string) (*schema.MetricDefinition, error) {
	// TODO: fetch from redis before checking elasticsearch
	if v, err := rs.Get(id).Result(); err != nil && err != redis.Nil {
		log.Printf("Error: the redis client bombed: %s", err.Error())
		return nil, err
	} else if err == nil {
		//fmt.Printf("json for %s found in redis\n", id)
		def, err := schema.MetricDefinitionFromJSON([]byte(v))
		if err != nil {
			return nil, err
		}
		return def, nil
	}

	log.Printf("checking elasticsearch for %s\n", id)
	res, err := es.Get("metric", "metric_index", id, nil)
	if err != nil {
		log.Printf("elasticsearch query failed. %s\n", err.Error())
		return nil, err
	}
	//fmt.Printf("elasticsearch query returned %q\n", res.Source)
	//fmt.Printf("placing %s into redis\n", id)
	if rerr := rs.SetEx(id, time.Duration(300)*time.Second, string(*res.Source)).Err(); err != nil {
		log.Printf("redis err: %s", rerr.Error())
	}

	def, err := schema.MetricDefinitionFromJSON(*res.Source)
	if err != nil {
		return nil, err
	}

	return def, nil
}
