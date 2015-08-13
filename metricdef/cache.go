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
	"errors"
	"fmt"
	"github.com/ctdk/goas/v2/logger"
	"sync"
)

var DefNotFound = errors.New("definition not found")

// Thar be redis-based locking. Worth investigating

// cache to hold metric definitions
type MetricDefCache struct {
	mdefs map[string]*MetricCacheItem
	m     sync.RWMutex
}

type MetricCacheItem struct {
	Def *MetricDefinition
	m   sync.RWMutex
}

func InitMetricDefCache() (*MetricDefCache, error) {
	mdc := new(MetricDefCache)
	mdc.mdefs = make(map[string]*MetricCacheItem)
	return mdc, nil
}

func (mdc *MetricDefCache) CheckMetricDef(m *IndvMetric) error {
	mdc.m.Lock()
	defer mdc.m.Unlock()
	_, exists := mdc.mdefs[m.Id]
	if !exists {
		fmt.Printf("%s not in local cache.\n", m.Id)
		def, err := GetMetricDefinition(m.Id)
		if err != nil {
			fmt.Println(err)
			if err.Error() == "record not found" {
				fmt.Printf("adding %s to metric defs\n", m.Id)
				def, err = NewFromMessage(m)
				if err != nil {
					return err
				}
			} else {
				return err
			}
		}
		mdc.mdefs[m.Id] = &MetricCacheItem{Def: def}
	}

	return nil
}

func (mdc *MetricDefCache) UpdateDefCache(mdef *MetricDefinition) error {
	mdc.m.Lock()
	defer mdc.m.Unlock()
	md, ok := mdc.mdefs[mdef.Id]

	if ok {
		md.Lock()
		defer md.Unlock()
		logger.Debugf("metric %s found", mdef.Id)
		if md.Def.LastUpdate >= mdef.LastUpdate {
			logger.Debugf("%s is already up to date", mdef.Id)
			return nil
		}
		mdc.mdefs[mdef.Id] = &MetricCacheItem{Def: mdef}
	} else {
		mdc.mdefs[mdef.Id] = &MetricCacheItem{Def: mdef}
	}
	return nil
}

func (mdc *MetricDefCache) RemoveDefCache(id string) {
	mdc.m.Lock()
	defer mdc.m.Unlock()
	md, ok := mdc.mdefs[id]
	if !ok {
		return
	}
	md.Lock()
	defer md.Unlock()
	delete(mdc.mdefs, id)
}

func (mdc *MetricDefCache) RemoveDefCacheLocked(id string) {
	mdc.m.Lock()
	defer mdc.m.Unlock()
	delete(mdc.mdefs, id)
}

func (mdc *MetricDefCache) GetDefItem(id string) (*MetricCacheItem, error) {
	mdc.m.RLock()
	defer mdc.m.RUnlock()
	def, ok := mdc.mdefs[id]
	if !ok {
		// try and get it from elasticsearch/redis
		d, err := GetMetricDefinition(id)
		if err != nil {
			return nil, err
		}
		def = &MetricCacheItem{Def: d}
	}
	return def, nil
}

func (mci *MetricCacheItem) Lock() {
	mci.m.Lock()
}

func (mci *MetricCacheItem) Unlock() {
	mci.m.Unlock()
}

func (mci *MetricCacheItem) RLock() {
	mci.m.RLock()
}

func (mci *MetricCacheItem) RUnlock() {
	mci.m.RUnlock()
}
