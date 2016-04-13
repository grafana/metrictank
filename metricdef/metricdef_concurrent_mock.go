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
	"github.com/raintank/raintank-metric/schema"
	"sync"
)

type DefsMockConcurrent struct {
	sync.Mutex
	defs map[string]*schema.MetricDefinition
}

func NewDefsMockConcurrent() *DefsMockConcurrent {
	return &DefsMockConcurrent{
		defs: make(map[string]*schema.MetricDefinition),
	}
}

// this does not mimic ES's scroll mechanism, we can cut this corner for now.
func (d *DefsMockConcurrent) GetMetrics(scroll_id string) ([]*schema.MetricDefinition, string, error) {
	out := make([]*schema.MetricDefinition, len(d.defs))
	i := 0
	d.Lock()
	for _, def := range d.defs {
		out[i] = def
		i++
	}
	d.Unlock()
	return out, "", nil
}

func (d *DefsMockConcurrent) IndexMetric(m *schema.MetricDefinition) error {
	d.Lock()
	d.defs[m.Id] = m
	d.Unlock()
	return nil
}

func (d *DefsMockConcurrent) GetMetricDefinition(id string) (*schema.MetricDefinition, bool, error) {
	if id == "" {
		panic("key cant be empty string.")
	}
	d.Lock()
	out, ok := d.defs[id]
	d.Unlock()
	return out, ok, nil
}

func (d *DefsMockConcurrent) Stop() {
}
