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
	"gopkg.in/redis.v2"
	"math/rand"
	"strconv"
	"sync"
	"time"
)

var DefNotFound = errors.New("definition not found")

// Thar be redis-based locking. Worth investigating

// cache to hold metric definitions
type MetricDefCache struct {
	mdefs    map[string]*MetricDefinition
	m        sync.RWMutex
	shortDur time.Duration
	longDur  time.Duration
	rs       *redis.Client
}

// a struct to hold metric definitions and their cached information, along with
// a mutex to keep data safe from concurrent access.
type MetricCacheItem struct {
	Def    *MetricDefinition
	Cache  *MetricCache
	m      sync.RWMutex
	parent *MetricDefCache
	id     string
	rl     *redisLock
}

type MetricCache struct {
	Raw struct {
		Data      []float64
		FlushTime int64
	}
	Aggr struct {
		Data struct {
			Avg []*float64
			Min []*float64
			Max []*float64
			Med []*float64
		}
		FlushTime int64
	}
}

// inspiration for the locking taken from
// https://github.com/atomic-labs/redislock/blob/master/redislock.go
type redisLock struct {
	id     string
	secret string
	rs     *redis.Client
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
	mdc.m.Lock()
	defer mdc.m.Unlock()

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
		mdc.mdefs[id] = def
	}

	// Fetch cache info from redis here, and if it doesn't exist:
	if rl, err := lockItem(mdc.rs, id); err != nil {
		if rl == nil {
			logger.Warningf("Couldn't get a redis lock for item %s", id)
			return nil
		}
		defer rl.unlockItem()
		c, err := mdc.getRedisCache(id)
		if err != nil {
			return err
		}
		if c == nil {
			c = mdc.initMetricCache()
		}

		// save the cached info here
		if err = mdc.setRedisCache(id, c); err != nil {
			return err
		}
	}

	return nil
}

func (mdc *MetricDefCache) initMetricCache() *MetricCache {
	c := &MetricCache{}
	now := time.Now().Unix()
	c.Raw.FlushTime = now - int64(mdc.shortDur/time.Second)
	c.Aggr.FlushTime = now - int64(mdc.longDur/time.Second)
	return c
}

func (mdc *MetricDefCache) UpdateDefCache(mdef *MetricDefinition) error {
	mdc.m.Lock()
	defer mdc.m.Unlock()
	md, ok := mdc.mdefs[mdef.Id]

	if ok {
		logger.Debugf("metric %s found", mdef.Id)
		if md.LastUpdate >= mdef.LastUpdate {
			logger.Debugf("%s is already up to date", mdef.Id)
			return nil
		}
	}
	// make sure the rollup info is in place
	if rl, err := lockItem(mdc.rs, mdef.Id); err != nil {
		if rl == nil {
			logger.Warningf("Couldn't get a redis lock for item %s when updating cache def", mdef.Id)
		} else {
			defer rl.unlockItem()
			if c, err := mdc.getRedisCache(mdef.Id); err != nil {
				return err
			} else if c == nil {
				c = mdc.initMetricCache()
				if err = mdc.setRedisCache(mdef.Id, c); err != nil {
					return err
				}
			}
		}
	}
	mdc.mdefs[mdef.Id] = mdef
	return nil
}

func (mdc *MetricDefCache) RemoveDefCache(id string) {
	mdc.m.Lock()
	defer mdc.m.Unlock()
	rl, err := lockItem(mdc.rs, id)
	if err != nil {
		logger.Errorf("Error getting redis lock while deleting %s: %s", id, err.Error())
		return
	} else if rl == nil {
		logger.Warningf("Can't get a lock to remove cache def %s, bailing", id)
		return
	}
	delete(mdc.mdefs, id)
	mdc.delRedisCache(id)
	rl.unlockItem()
}

func (mdc *MetricDefCache) RemoveDefFromMap(id string) {
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
		var err error
		def, err = GetMetricDefinition(id)
		if err != nil {
			return nil, err
		}
	}
	rl, err := lockItem(mdc.rs, id)
	if err != nil {
		return nil, err
	} else if rl == nil {
		return nil, fmt.Errorf("couldn't get redis lock for def item %s", id)
	}
	c, err := mdc.getRedisCache(id)
	if err != nil {
		rl.unlockItem()
		return nil, err
	} else if c == nil {
		logger.Debugf("Nothing found for %s in metric def redis cache for %s", id)
		c = mdc.initMetricCache()
		if err = mdc.setRedisCache(id, c); err != nil {
			rl.unlockItem()
			return nil, err
		}
	}
	return &MetricCacheItem{Def: def, Cache: c, parent: mdc, id: id, rl: rl}, nil
}

func (mci *MetricCacheItem) Save() error {
	mci.parent.m.RLock()
	defer mci.parent.m.RUnlock()
	defer mci.rl.unlockItem()
	if err := mci.parent.setRedisCache(mci.id, mci.Cache); err != nil {
		return err
	}
	return nil
}

func (mci *MetricCacheItem) Release() {
	mci.rl.unlockItem()
}

func (mdc *MetricDefCache) getRedisCache(id string) (*MetricCache, error) {
	v, err := mdc.rs.Get(redisCacheId(id)).Result()
	if err != nil && err != redis.Nil {
		logger.Errorf("Getting metric cache info failed: %s", err.Error())
		return nil, err
	} else if err == redis.Nil {
		return nil, nil
	}
	c := new(MetricCache)
	if err = json.Unmarshal([]byte(v), &c); err != nil {
		return nil, err
	}
	return c, nil
}

func (mdc *MetricDefCache) setRedisCache(id string, c *MetricCache) error {
	j, err := json.Marshal(c)
	if err != nil {
		return err
	}
	if err = mdc.rs.Set(redisCacheId(id), string(j)).Err(); err != nil {
		return err
	}
	return nil
}

func (mdc *MetricDefCache) delRedisCache(id string) {
	mdc.rs.Del(redisCacheId(id))
}

func (mci *MetricCacheItem) Lock() {
	mci.m.Lock()
}

func (mci *MetricCacheItem) Unlock() {
	mci.m.Unlock()
}

func redisCacheId(id string) string {
	return fmt.Sprintf("cache:%s", id)
}

func (r *redisLock) key() string {
	return fmt.Sprintf("lock:%s", r.id)
}

func lockItem(rs *redis.Client, id string) (*redisLock, error) {
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	s := strconv.FormatInt(rnd.Int63(), 16)
	r := &redisLock{id: id, secret: s, rs: rs}
	errCh := make(chan error, 1)
	stop := make(chan struct{})
	go func(r *redisLock, stop chan struct{}) {
		for {
			select {
			case <-stop:
				return
			default:
				if err := rs.SetEx(r.key(), 5*time.Second, r.secret).Err(); err != nil {
					// If trying to get a lock returned an error
					// besides redis.Nil, return the error. If it
					// was redis nil, try again.
					if err != redis.Nil {
						errCh <- err
						return
					}
				} else {
					// got the lock
					break
				}
			}
		}
		errCh <- nil
	}(r, stop)

	select {
	case err := <-errCh:
		if err != nil {
			return nil, err
		}
	case <-time.After(5 * time.Second):
		logger.Infof("Could not get redis lock for %s", id)
		stop <- struct{}{}
		return nil, nil
	}
	return r, nil
}

func (r *redisLock) unlockItem() error {
	unlock := redis.NewScript(`
	if redis.call("get", KEYS[1]) == ARGV[1]
		then
			return redis.call("del", KEYS[1])
		else
			return 0
		end
	`)
	return unlock.Run(rs, []string{r.key()}, []string{r.secret}).Err()
}
