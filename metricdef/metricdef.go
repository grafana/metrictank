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
	"errors"
	"fmt"
	"github.com/ctdk/goas/v2/logger"
	elastigo "github.com/mattbaird/elastigo/lib"
	"gopkg.in/redis.v2"
	"reflect"
	"strconv"
	"sync"
	"time"
)

// Monitor states
const (
	StateOK int8 = iota
	StateWarn
	StateCrit
)

var LevelMap = [...]string{"ok", "warning", "critical"}

type MetricDefinition struct {
	Id         string `json:"id"`
	Name       string `json:"name" elastic:"type:string,index:not_analyzed"`
	OrgId      int    `json:"org_id"`
	Metric     string `json:"metric"`
	TargetType string `json:"target_type"` // an emum ["derive","gauge"] in nodejs
	Unit       string `json:"unit"`
	Interval   int    `json:"interval"`   // minimum 10
	LastUpdate int64  `json:"lastUpdate"` // unix epoch time, per the nodejs definition
	Thresholds struct {
		WarnMin interface{} `json:"warnMin"`
		WarnMax interface{} `json:"warnMax"`
		CritMin interface{} `json:"critMin"`
		CritMax interface{} `json:"critMax"`
	} `json:"thresholds"`
	KeepAlives int                    `json:"keepAlives"`
	State      int8                   `json:"state"`
	Extra      map[string]interface{} `json:"-"`
	m sync.RWMutex					  `json:"-"`
}

// The JSON marshal/unmarshal with metric definitions is a little less
// complicated than it is with the event definitions. The main wrinkle is that
// there are two fields that should be in the metric definition struct that
// can't be required, but on the other hand it doesn't need to coerce any float
// into in64, because floats are reasonable values here.
// Anything though that's not state or keepAlives gets stuffed into Extra in
// metric definitions, in any case.

type requiredField struct {
	StructName string
	Seen       bool
}

func (m *MetricDefinition) UnmarshalJSON(raw []byte) error {
	//lets start by unmashaling into a basic map datastructure
	metric := make(map[string]interface{})
	err := json.Unmarshal(raw, &metric)
	if err != nil {
		return err
	}

	//lets get a list of our required fields.
	s := reflect.TypeOf(*m)
	requiredFields := make(map[string]*requiredField)

	for i := 0; i < s.NumField(); i++ {
		field := s.Field(i)
		if field.PkgPath != "" {
			continue
		}
		name := field.Name
		// look at the field Tags to work out the property named used in the
		// JSON document.
		tag := field.Tag.Get("json")
		if tag != "" && tag != "-" {
			name = tag
		}
		//all fields except 'Extra', 'Id', "KeepAlives", and "state"
		// are required.
		if name != "Extra" && name != "id" && name != "keepAlives" && name != "state" {
			requiredFields[name] = &requiredField{
				StructName: field.Name,
				Seen:       false,
			}
		}
	}

	m.Extra = make(map[string]interface{})
	for k, v := range metric {
		def, ok := requiredFields[k]
		// anything that is not a required field gets
		// stored in our 'Extra' field.
		if !ok {
			m.Extra[k] = v
		} else {
			switch reflect.ValueOf(m).Elem().FieldByName(def.StructName).Kind() {
			case reflect.Int:
				v = int(v.(float64))
			case reflect.Int8:
				v = int8(v.(float64))
			case reflect.Int64:
				v = int64(v.(float64))
			case reflect.Struct:
				y := v.(map[string]interface{})
				v = struct {
					WarnMin interface{} `json:"warnMin"`
					WarnMax interface{} `json:"warnMax"`
					CritMin interface{} `json:"critMin"`
					CritMax interface{} `json:"critMax"`
				}{
					y["warnMin"],
					y["warnMax"],
					y["critMix"],
					y["critMax"],
				}
			}
			value := reflect.ValueOf(v)
			if value.IsValid() {
				reflect.ValueOf(m).Elem().FieldByName(def.StructName).Set(value)
			} else {
				logger.Warningf("Yikes, in metricdef %s had the zero value! %v", k, v)
			}
			def.Seen = true
		}
	}

	//make sure all required fields were present.
	for _, v := range requiredFields {
		if !v.Seen && !(v.StructName == "State" || v.StructName == "KeepAlives") {
			return fmt.Errorf("Required field '%s' missing", v.StructName)
		}
	}
	return nil
}

func (m *MetricDefinition) MarshalJSON() ([]byte, error) {
	metric := make(map[string]interface{})

	value := reflect.ValueOf(*m)
	for i := 0; i < value.Type().NumField(); i++ {
		field := value.Type().Field(i)
		if field.PkgPath != "" {
			continue
		}
		name := field.Name
		if name == "Extra" {
			//anything that was in Extra[] becomes a toplevel property again.
			for k, v := range m.Extra {
				metric[k] = v
			}
		} else {
			tag := field.Tag.Get("json")
			if tag != "" && tag != "-" {
				name = tag
			}
			v, err := encode(value.FieldByName(field.Name))
			if err != nil {
				return nil, err
			}
			metric[name] = v
		}
	}
	//Marshal our map[string] into a JSON string (byte[]).
	raw, err := json.Marshal(&metric)
	if err != nil {
		return nil, err
	}
	return raw, nil
}

func encode(v reflect.Value) (interface{}, error) {
	switch v.Type().Kind() {
	case reflect.Bool:
		return v.Bool(), nil
	case reflect.String:
		return v.String(), nil
	case reflect.Int, reflect.Int8, reflect.Int64:
		return v.Int(), nil
	case reflect.Float64:
		return v.Float(), nil
	case reflect.Struct:
		return v.Interface(), nil
	default:
		return nil, errors.New("Unsupported type")
	}
}

var es *elastigo.Conn

func InitElasticsearch(domain string, port int, user, pass string) error {
	es = elastigo.NewConn()
	es.Domain = domain // needs to be configurable obviously
	es.Port = strconv.Itoa(port)
	if user != "" && pass != "" {
		es.Username = user
		es.Password = pass
	}
	if exists, err := es.ExistsIndex("definitions", "metric", nil); err != nil && err.Error() != "record not found" {
		return err
	} else {
		if !exists {
			_, err = es.CreateIndex("definitions")
			if err != nil {
				return err
			}
		}
		esopts := elastigo.MappingOptions{}

		err = es.PutMapping("definitions", "metric", MetricDefinition{}, esopts)
		if err != nil {
			return err
		}
	}

	return nil
}

var rs *redis.Client

func InitRedis(addr, passwd string, db int64) error {
	opts := &redis.Options{}
	opts.Network = "tcp"
	opts.Addr = addr
	if passwd != "" {
		opts.Password = passwd
	}
	opts.DB = db
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
	id := m.Id
	now := time.Now().Unix()

	var ka int
	switch k := m.Extra["keepAlives"].(type) {
	case float64:
		ka = int(k)
	}
	var state int8
	switch s := m.Extra["state"].(type) {
	case float64:
		state = int8(s)
	}

	// input is now validated by json unmarshal

	def := &MetricDefinition{Id: id,
		Name:       m.Name,
		OrgId:      m.OrgId,
		Metric:     m.Metric,
		TargetType: m.TargetType,
		Interval:   m.Interval,
		LastUpdate: now,
		KeepAlives: ka,
		State:      state,
		Unit:       m.Unit,
		Extra:      m.Extra,
	}

	if t, exists := m.Extra["thresholds"]; exists {
		thresh, _ := t.(map[string]interface{})
		for k, v := range thresh {
			switch k {
			case "warnMin":
				def.Thresholds.WarnMin = int(v.(float64))
			case "warnMax":
				def.Thresholds.WarnMax = int(v.(float64))
			case "critMin":
				def.Thresholds.CritMin = int(v.(float64))
			case "critMax":
				def.Thresholds.CritMax = int(v.(float64))
			}
		}
	}

	err := def.Save()
	if err != nil {
		return nil, err
	}

	return def, nil
}

func (m *MetricDefinition) Save() error {
	if m.Id == "" {
		m.Id = fmt.Sprintf("%d.%s", m.OrgId, m.Name)
	}
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
	resp, err := es.Index("definitions", "metric", m.Id, nil, m)
	logger.Debugf("response ok? %v", resp.Ok)
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
		logger.Debugf("json for %s found in elasticsearch: %s", id)
		def, err := DefFromJSON([]byte(v))
		if err != nil {
			return nil, err
		}
		return def, nil
	}

	logger.Debugf("getting %s from elasticsearch", id)
	res, err := es.Get("definitions", "metric", id, nil)
	logger.Debugf("res is: %+v", res)
	if err != nil {
		return nil, err
	}
	logger.Debugf("get returned %q", res.Source)
	logger.Debugf("placing %s into redis", id)
	if rerr := rs.SetEx(id, time.Duration(300)*time.Second, string(*res.Source)).Err(); err != nil {
		logger.Debugf("redis err: %s", rerr.Error())
	}

	def, err := DefFromJSON(*res.Source)
	if err != nil {
		return nil, err
	}

	return def, nil
}
