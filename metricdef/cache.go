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
	"gopkg.in/redis.v2"
	"sync"
	"time"
	"github.com/ctdk/goas/v2/logger"
	"errors"
)

var DefNotFound = errors.New("definition not found")

// Thar be redis-based locking. Worth investigating

// cache to hold metric definitions
type MetricDefCache struct {
	mdefs map[string]*MetricDefinition
	m sync.RWMutex
	shortDur time.Duration
	longDur time.Duration
	rs *redis.Client
}

// a struct to hold metric definitions and their cached information, along with
// a mutex to keep data safe from concurrent access.
type MetricCacheItem struct {
	Def  *MetricDefinition
	Cache *MetricCache
	m     sync.RWMutex
}

type MetricCache struct {
	Raw struct {
		Data []float64
		FlushTime int64
	}
	Aggr struct {
		Data struct {
			Avg []*float64
			Min []*float64
			Max []*float64
		}
		FlushTime int64
	}
}

func InitMetricDefCache(shortDur, longDur time.Duration, addr, passwd string, db int64) (*MetricDefCache, error) {
	mdc := new(MetricDefCache)
	mdc.mdefs = make(map[string]*MetricDefinition)
	mdc.shortDur = shortDur
	mdc.longDur = longDur
	opts := &redis.Options{}
	opts.Network = "tcp"
	opts.Addr = addr
	if passwd != "" {
		opts.Password = passwd
	}
	opts.DB = db
	mdc.rs = redis.NewClient(opts)
	return mdc, nil
}

func (mdc *MetricDefCache) CheckMetricDef(id string, m *IndvMetric) error {
	mdc.m.RLock()

	// Fetch cache info from redis here, and if it doesn't exist:
	var c *MetricCache 
	if false {

	} else {
		c = &MetricCache{}
		now := time.Now().Unix()
		c.Raw.FlushTime = now - int64(mdc.shortDur)
		c.Aggr.FlushTime = now - int64(mdc.longDur)
	}

	def, exists := mdc.mdefs[id]
	if !exists {
		var err error
		def, err = GetMetricDefinition(id)
		if err != nil {
			if err.Error() == "record not found" {
				logger.Debugf("adding %s to metric defs", id)
				def, err = NewFromMessage(m)
				if err != nil {
					return err
				}
			} else {
				return err
			}
		}
		mdc.m.RUnlock()
		mdc.m.Lock()
		mdc.mdefs[id] = def
		mdc.m.Unlock()
	} else {
		mdc.m.RUnlock()
	}

	md := &MetricCacheItem{ Def: def, Cache: c }
	_ = md

	// save the cached info here

	return nil
}

func (mdc *MetricDefCache) UpdateDefCache(mdef *MetricDefinition) error {
	mdc.m.Lock()
	defer mdc.m.Unlock()
	md, ok := mdc.mdefs[mdef.Id]
	// get cache from redis
	if ok {
		logger.Debugf("metric %s found", mdef.Id)

	}
}

func (mdc *MetricDefCache) RemoveDefCache(id string) {
	logger.Debugf("Removing metric def for %s", id)
	mdc.m.Lock()
	defer mdc.m.Unlock()
	md, ok := mdc.mdefs[id]
	if !ok {
		return
	}
	md.m.Lock()
	defer md.m.Unlock()
	delete(mdc.mdefs, id)
	return
}

func (mdc *MetricDefCache) GetDefItem(id string) (*MetricCacheItem, error) {

}

func (mci *MetricCacheItem) Lock() {
	mci.m.Lock()
}

func (mci *MetricCacheItem) Unlock() {
	mci.m.Unlock()
}
