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
	"github.com/ctdk/goas/v2/logger"
	elastigo "github.com/mattbaird/elastigo/lib"
	"github.com/raintank/raintank-metric/setting"
	"gopkg.in/redis.v2"
	"sort"
	"strconv"
	"sync"
	"time"
)

type MetricDefinition struct {
	Id         string                 `json:"id"`
	Name       string                 `json:"name" elastic:"type:string,index:not_analyzed"`
	OrgId      int                    `json:"org_id"`
	Metric     string                 `json:"metric"`
	TargetType string                 `json:"target_type"` // an emum ["derive","gauge"] in nodejs
	Unit       string                 `json:"unit"`
	Interval   int                    `json:"interval"`   // minimum 10
	LastUpdate int64                  `json:"lastUpdate"` // unix epoch time, per the nodejs definition
	Tags       map[string]interface{} `json:"tags"`
	m          sync.RWMutex           `json:"-"`
}

type IndvMetric struct {
	Id         string                 `json:"id"`
	OrgId      int                    `json:"org_id"`
	Name       string                 `json:"name"`
	Metric     string                 `json:"metric"`
	Interval   int                    `json:"interval"`
	Value      float64                `json:"value"`
	Unit       string                 `json:"unit"`
	Time       int64                  `json:"time"`
	TargetType string                 `json:"target_type"`
	Tags       map[string]interface{} `json:"tags"`
}

func (m *IndvMetric) SetId() {
	if m.Id != "" {
		//id already set.
		return
	}
	var buffer bytes.Buffer
	keys := make([]string, 0)
	for k, _ := range m.Tags {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		buffer.WriteString(fmt.Sprintf(":%s=%v", k, m.Tags[k]))
	}

	m.Id = fmt.Sprintf("%d.%s%s", m.OrgId, m.Name, buffer.String())
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

		err = es.PutMapping("metric", "metric_index", MetricDefinition{}, esopts)
		if err != nil {
			return err
		}
	}

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

// required: name, org_id, target_type, interval, metric, unit

// These validate, and save to elasticsearch

func DefFromJSON(b []byte) (*MetricDefinition, error) {
	def := new(MetricDefinition)
	if err := json.Unmarshal(b, &def); err != nil {
		return nil, err
	}
	def.Id = fmt.Sprintf("%d.%s", def.OrgId, def.Name)
	return def, nil
}

func NewFromMessage(m *IndvMetric) (*MetricDefinition, error) {
	logger.Debugf("incoming message: %+v", m)
	now := time.Now().Unix()

	// input is now validated by json unmarshal
	def := &MetricDefinition{
		Id:         "",
		Name:       m.Name,
		OrgId:      m.OrgId,
		Metric:     m.Metric,
		TargetType: m.TargetType,
		Interval:   m.Interval,
		LastUpdate: now,
		Unit:       m.Unit,
		Tags:       m.Tags,
	}
	err := def.Save()
	if err != nil {
		return nil, err
	}

	return def, nil
}

func (m *MetricDefinition) SetId() {
	if m.Id != "" {
		//id already set.
		return
	}
	var buffer bytes.Buffer
	keys := make([]string, 0)
	for k, _ := range m.Tags {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		buffer.WriteString(fmt.Sprintf(":%s=%v", k, m.Tags[k]))
	}

	m.Id = fmt.Sprintf("%d.%s%s", m.OrgId, m.Name, buffer.String())
}

func (m *MetricDefinition) Save() error {
	m.SetId()
	if m.LastUpdate == 0 {
		m.LastUpdate = time.Now().Unix()
	}
	if err := m.validate(); err != nil {
		return err
	}
	// save in elasticsearch
	return m.indexMetric()
}

func (m *MetricDefinition) Update() error {
	if err := m.validate(); err != nil {
		return err
	}
	// save in elasticsearch
	return m.indexMetric()
}

func (m *MetricDefinition) validate() error {
	if m.Name == "" || m.OrgId == 0 || (m.TargetType != "derive" && m.TargetType != "gauge") || m.Interval == 0 || m.Metric == "" || m.Unit == "" {
		// TODO: this error message ought to be more informative
		err := fmt.Errorf("metric is not valid!")
		return err
	}
	return nil
}

func (m *MetricDefinition) indexMetric() error {
	resp, err := es.Index("metric", "metric_index", m.Id, nil, m)
	fmt.Printf("response ok? %v", resp.Ok)
	if err != nil {
		return err
	}
	return nil
}

func GetMetricDefinition(id string) (*MetricDefinition, error) {
	// TODO: fetch from redis before checking elasticsearch
	if v, err := rs.Get(id).Result(); err != nil && err != redis.Nil {
		logger.Errorf("the redis client bombed: %s", err.Error())
		return nil, err
	} else if err == nil {
		fmt.Printf("json for %s found in redis\n", id)
		def, err := DefFromJSON([]byte(v))
		if err != nil {
			return nil, err
		}
		return def, nil
	}

	fmt.Printf("getting %s from elasticsearch\n", id)
	res, err := es.Get("metric", "metric_index", id, nil)
	fmt.Printf("res is: %+v\n", res)
	if err != nil {
		return nil, err
	}
	fmt.Printf("get returned %q\n", res.Source)
	fmt.Printf("placing %s into redis", id)
	if rerr := rs.SetEx(id, time.Duration(300)*time.Second, string(*res.Source)).Err(); err != nil {
		fmt.Printf("redis err: %s", rerr.Error())
	}

	def, err := DefFromJSON(*res.Source)
	if err != nil {
		return nil, err
	}

	return def, nil
}
