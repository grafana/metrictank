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
	"fmt"
	"encoding/json"
	"reflect"
	"github.com/ctdk/goas/v2/logger"
)

// IndvMetric holds the information from an individual metric item coming in 
// from rabbitmq.
type IndvMetric struct {
	Id         string `json:"id"`
	OrgId      int `json:"org_id"`
	Name       string `json:"name"`
	Metric     string `json:"metric"`
	Interval   int `json:"interval"`
	Value      float64 `json:"value"`
	Unit       string `json:"unit"`
	Time       int64 `json:"time"`
	TargetType string `json:"target_type"`
	Extra map[string]interface{} `json:"-"`
}

func (m *IndvMetric) UnmarshalJSON(raw []byte) error {
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
		name := field.Name
		// look at the field Tags to work out the property named used in the
		// JSON document.
		tag := field.Tag.Get("json")
		if tag != "" && tag != "-" {
			name = tag
		}
		//all fields except 'Extra', 'Id'
		// are required.
		if name != "Extra" && name != "id" {
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
			case reflect.Int64:
				v = int64(v.(float64))
			}
			value := reflect.ValueOf(v)
			if value.IsValid() {
				reflect.ValueOf(m).Elem().FieldByName(def.StructName).Set(value)
			} else {
				logger.Warningf("Yikes, in indvmetric %s had the zero value! %v", k, v)
			}
			def.Seen = true
		}
	}

	//make sure all required fields were present.
	for _, v := range requiredFields {
		if !v.Seen {
			return fmt.Errorf("Required field '%s' missing", v.StructName)
		}
	}
	return nil
}

func (m *IndvMetric) MarshalJSON() ([]byte, error) {
	metric := make(map[string]interface{})

	value := reflect.ValueOf(*m)
	for i := 0; i < value.Type().NumField(); i++ {
		field := value.Type().Field(i)
		name := field.Name
		tag := field.Tag.Get("json")
		if tag != "" && tag != "-" {
			name = tag
		}
		if name == "Extra" {
			//anything that was in Extra[] becomes a toplevel property again.
			for k, v := range m.Extra {
				metric[k] = v
			}
		} else {
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
