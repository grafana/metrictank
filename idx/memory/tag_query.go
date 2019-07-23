package memory

import (
	"math"
	"regexp"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/grafana/metrictank/expr/tagquery"
	"github.com/grafana/metrictank/idx"
	"github.com/raintank/schema"
	log "github.com/sirupsen/logrus"
)

// the supported operators are documented together with the graphite
// reference implementation:
// http://graphite.readthedocs.io/en/latest/tags.html
//
// some of the following operators are non-standard and are only used
// internally to implement certain functionalities requiring them

// a key / value combo used to represent a tag expression like "key=value"
// the cost is an estimate how expensive this query is compared to others
// with the same operator
type kv struct {
	tagquery.Tag
	cost uint // cost of evaluating expression, compared to other kv objects
}

// kv expressions that rely on regular expressions will get converted to kvRe in
// NewTagQueryContext() to accommodate the additional requirements of regex queries
type kvRe struct {
	kv
	Regex          *regexp.Regexp
	matchCache     *sync.Map // needs to be reference so kvRe can be copied, caches regex matches
	matchCacheSize int32     // sync.Map does not have a way to get the length
	missCache      *sync.Map // needs to be reference so kvRe can be copied, caches regex misses
	missCacheSize  int32     // sync.Map does not have a way to get the length
}

type KvByCost []kv

func (a KvByCost) Len() int           { return len(a) }
func (a KvByCost) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a KvByCost) Less(i, j int) bool { return a[i].cost < a[j].cost }

type KvReByCost []kvRe

func (a KvReByCost) Len() int           { return len(a) }
func (a KvReByCost) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a KvReByCost) Less(i, j int) bool { return a[i].cost < a[j].cost }

// TagQueryContext runs a set of pattern or string matches on tag keys and values against
// the index. It is executed via:
// Run() which returns a set of matching MetricIDs
// RunGetTags() which returns a list of tags of the matching metrics
type TagQueryContext struct {
	// clause that operates on LastUpdate field
	from int64

	// clauses that operate on values. from expressions like tag<operator>value
	equal    []kv   // EQUAL
	match    []kvRe // MATCH
	notEqual []kv   // NOT_EQUAL
	notMatch []kvRe // NOT_MATCH
	prefix   []kv   // PREFIX

	index     TagIndex                             // the tag index, hierarchy of tags & values, set by Run()/RunGetTags()
	byId      map[schema.MKey]*idx.ArchiveInterned // the metric index by ID, set by Run()/RunGetTags()
	tagClause tagquery.ExpressionOperator          // to know the clause type. either PREFIX_TAG or MATCH_TAG (or 0 if unset)
	tagMatch  kvRe                                 // only used for /metrics/tags with regex in filter param
	tagPrefix string                               // only used for auto complete of tags to match exact prefix
	startWith tagquery.ExpressionOperator          // choses the first clause to generate the initial result set (one of EQUAL PREFIX MATCH MATCH_TAG PREFIX_TAG)
	wg        *sync.WaitGroup
}

// NewTagQueryContext takes a tag query and wraps it into all the
// context structs necessary to execute the query on the index
func NewTagQueryContext(query tagquery.Query) TagQueryContext {
	kvsFromExpressions := func(expressions []tagquery.Expression) []kv {
		res := make([]kv, len(expressions))
		for i := range expressions {
			res[i] = kv{Tag: expressions[i].Tag}
		}
		return res
	}

	kvReFromExpression := func(expression tagquery.Expression) kvRe {
		return kvRe{
			kv:         kv{Tag: expression.Tag},
			Regex:      expression.Regex,
			matchCache: &sync.Map{},
			missCache:  &sync.Map{},
		}
	}

	kvResFromExpressions := func(expressions []tagquery.Expression) []kvRe {
		res := make([]kvRe, len(expressions))
		for i := range expressions {
			res[i] = kvReFromExpression(expressions[i])
		}
		return res
	}

	return TagQueryContext{
		wg:        &sync.WaitGroup{},
		equal:     kvsFromExpressions(query.Expressions[tagquery.EQUAL]),
		match:     kvResFromExpressions(query.Expressions[tagquery.MATCH]),
		notEqual:  kvsFromExpressions(query.Expressions[tagquery.NOT_EQUAL]),
		notMatch:  kvResFromExpressions(query.Expressions[tagquery.NOT_MATCH]),
		prefix:    kvsFromExpressions(query.Expressions[tagquery.PREFIX]),
		tagClause: query.TagClause,
		tagPrefix: query.TagPrefix,
		tagMatch:  kvReFromExpression(query.TagMatch),
		startWith: query.StartWith,
		from:      query.From,
	}
}

// getInitialByEqual generates the initial resultset by executing the given equal expression
func (q *TagQueryContext) getInitialByEqual(key, value uintptr, idCh chan schema.MKey, stopCh chan struct{}) {
	defer q.wg.Done()

KEYS:
	for k := range q.index[key][value] {
		select {
		case <-stopCh:
			break KEYS
		case idCh <- k:
		}
	}

	close(idCh)
}

// getInitialByPrefix generates the initial resultset by executing the given prefix match expression
func (q *TagQueryContext) getInitialByPrefix(key uintptr, exprvalue string, idCh chan schema.MKey, stopCh chan struct{}) {
	defer q.wg.Done()

VALUES:
	for v, ids := range q.index[key] {
		value, err := idx.IdxIntern.GetStringFromPtr(v)
		if err != nil {
			log.Error("memory-idx: Failed to retrieve uintptr for interned tag value: ", err)
			internError.Inc()
			break VALUES
		}
		if !strings.HasPrefix(value, exprvalue) {
			continue
		}

		for id := range ids {
			select {
			case <-stopCh:
				break VALUES
			case idCh <- id:
			}
		}
	}

	close(idCh)
}

// getInitialByMatch generates the initial resultset by executing the given match expression
func (q *TagQueryContext) getInitialByMatch(key uintptr, expr kvRe, idCh chan schema.MKey, stopCh chan struct{}) {
	defer q.wg.Done()

	// shortcut if Regex == nil.
	// this will simply match any value, like ^.+. since we know that every value
	// in the index must not be empty, we can skip the matching.
	if expr.Regex == nil {
	VALUES1:
		for _, ids := range q.index[key] {
			for id := range ids {
				select {
				case <-stopCh:
					break VALUES1
				case idCh <- id:
				}
			}
		}
		close(idCh)
		return
	}

VALUES2:
	for v, ids := range q.index[key] {
		value, err := idx.IdxIntern.GetStringFromPtr(v)
		if err != nil {
			log.Error("memory-idx: Failed to retrieve uintptr for interned tag value: ", err)
			internError.Inc()
			break VALUES2
		}
		if !expr.Regex.MatchString(value) {
			continue
		}

		for id := range ids {
			select {
			case <-stopCh:
				break VALUES2
			case idCh <- id:
			}
		}
	}

	close(idCh)
}

// getInitialByTagPrefix generates the initial resultset by creating a list of
// metric IDs of which at least one tag starts with the defined prefix
func (q *TagQueryContext) getInitialByTagPrefix(idCh chan schema.MKey, stopCh chan struct{}) {
	defer q.wg.Done()

TAGS:
	for tag, values := range q.index {
		key, err := idx.IdxIntern.GetStringFromPtr(tag)
		if err != nil {
			log.Error("memory-idx: Failed to retrieve uintptr for interned tag key: ", err)
			internError.Inc()
			break TAGS
		}
		if !strings.HasPrefix(key, q.tagPrefix) {
			continue
		}

		for _, ids := range values {
			for id := range ids {
				select {
				case <-stopCh:
					break TAGS
				case idCh <- id:
				}
			}
		}
	}

	close(idCh)
}

// getInitialByTagMatch generates the initial resultset by creating a list of
// metric IDs of which at least one tag matches the defined regex
func (q *TagQueryContext) getInitialByTagMatch(idCh chan schema.MKey, stopCh chan struct{}) {
	defer q.wg.Done()

TAGS:
	for tag, values := range q.index {
		key, err := idx.IdxIntern.GetStringFromPtr(tag)
		if err != nil {
			log.Error("memory-idx: Failed to retrieve uintptr for interned tag key: ", err)
			internError.Inc()
			break TAGS
		}
		if q.tagMatch.Regex.MatchString(key) {
			for _, ids := range values {
				for id := range ids {
					select {
					case <-stopCh:
						break TAGS
					case idCh <- id:
					}
				}
			}
		}
	}

	close(idCh)
}

// getInitialIds asynchronously collects all ID's of the initial result set.  It returns:
// a channel through which the IDs of the initial result set will be sent
// a stop channel, which when closed, will cause it to abort the background worker.
func (q *TagQueryContext) getInitialIds() (chan schema.MKey, chan struct{}) {
	idCh := make(chan schema.MKey, 1000)
	stopCh := make(chan struct{})
	q.wg.Add(1)

	switch q.startWith {
	case tagquery.EQUAL:
		query := q.equal[0]
		q.equal = q.equal[1:]
		key, err := idx.IdxIntern.GetPtrFromByte([]byte(query.Key))
		if err != nil {
			log.Error("memory-idx: Failed to retrieve uintptr for interned tag key: ", err)
			internError.Inc()
			q.wg.Done()
			return nil, nil
		}
		value, err := idx.IdxIntern.GetPtrFromByte([]byte(query.Value))
		if err != nil {
			log.Error("memory-idx: Failed to retrieve uintptr for interned tag value: ", err)
			internError.Inc()
			q.wg.Done()
			return nil, nil
		}
		go q.getInitialByEqual(key, value, idCh, stopCh)
	case tagquery.PREFIX:
		var err error
		query := q.prefix[0]
		q.prefix = q.prefix[1:]
		key, err := idx.IdxIntern.GetPtrFromByte([]byte(query.Key))
		if err != nil {
			log.Error("memory-idx: Failed to retrieve uintptr for interned tag key: ", err)
			internError.Inc()
			q.wg.Done()
			return nil, nil
		}
		go q.getInitialByPrefix(key, query.Value, idCh, stopCh)
	case tagquery.MATCH:
		query := q.match[0]
		q.match = q.match[1:]
		key, err := idx.IdxIntern.GetPtrFromByte([]byte(query.Key))
		if err != nil {
			log.Error("memory-idx: Failed to retrieve uintptr for interned tag key: ", err)
			internError.Inc()
			q.wg.Done()
			return nil, nil
		}
		go q.getInitialByMatch(key, query, idCh, stopCh)
	case tagquery.PREFIX_TAG:
		go q.getInitialByTagPrefix(idCh, stopCh)
	case tagquery.MATCH_TAG:
		go q.getInitialByTagMatch(idCh, stopCh)
	}

	return idCh, stopCh
}

// testByAllExpressions takes and id and a MetricDefinition and runs it through
// all required tests in order to decide whether this metric should be part
// of the final result set or not
// in map/reduce terms this is the reduce function
func (q *TagQueryContext) testByAllExpressions(id schema.MKey, def *idx.ArchiveInterned, omitTagFilters bool) bool {
	if !q.testByFrom(def) {
		return false
	}

	if len(q.equal) > 0 && !q.testByEqual(id, q.equal, false) {
		return false
	}

	if len(q.notEqual) > 0 && !q.testByEqual(id, q.notEqual, true) {
		return false
	}

	if q.tagClause == tagquery.PREFIX_TAG && !omitTagFilters && q.startWith != tagquery.PREFIX_TAG {
		if !q.testByTagPrefix(def) {
			return false
		}
	}

	if !q.testByPrefix(def, q.prefix) {
		return false
	}

	if q.tagClause == tagquery.MATCH_TAG && !omitTagFilters && q.startWith != tagquery.MATCH_TAG {
		if !q.testByTagMatch(def) {
			return false
		}
	}

	if len(q.match) > 0 && !q.testByMatch(def, q.match, false) {
		return false
	}

	if len(q.notMatch) > 0 && !q.testByMatch(def, q.notMatch, true) {
		return false
	}

	return true
}

// testByMatch filters a given metric by matching a regular expression against
// the values of specific associated tags
func (q *TagQueryContext) testByMatch(def *idx.ArchiveInterned, exprs []kvRe, not bool) bool {
EXPRS:
	for _, e := range exprs {
		if e.Key == "name" {
			if e.Regex == nil || e.Regex.MatchString(schema.SanitizeNameAsTagValue(def.Name.String())) {
				if not {
					return false
				}
				continue EXPRS
			} else {
				if !not {
					return false
				}
				continue EXPRS
			}
		}

		for _, tag := range def.Tags.KeyValues {
			key, err := idx.IdxIntern.GetStringFromPtr(tag.Key)
			if err != nil {
				log.Error("memory-idx: Failed to retrieve uintptr for interned tag key: ", err)
				internError.Inc()
				continue
			}
			if key != e.Key {
				continue
			}

			// reduce regex matching by looking up cached non-matches
			if _, ok := e.missCache.Load(tag.Value); ok {
				continue
			}

			// reduce regex matching by looking up cached matches
			if _, ok := e.matchCache.Load(tag.Value); ok {
				if not {
					return false
				}
				continue EXPRS
			}

			// Regex == nil means that this expression can be short cut
			// by not evaluating it
			value, err := idx.IdxIntern.GetStringFromPtr(tag.Value)
			if err != nil {
				log.Error("memory-idx: Failed to retrieve uintptr for interned tag value: ", err)
				internError.Inc()
				continue
			}
			if e.Regex == nil || e.Regex.MatchString(value) {
				if atomic.LoadInt32(&e.matchCacheSize) < int32(matchCacheSize) {
					e.matchCache.Store(tag.Value, struct{}{})
					atomic.AddInt32(&e.matchCacheSize, 1)
				}
				if not {
					return false
				}
				continue EXPRS
			} else {
				if atomic.LoadInt32(&e.missCacheSize) < int32(matchCacheSize) {
					e.missCache.Store(tag.Value, struct{}{})
					atomic.AddInt32(&e.missCacheSize, 1)
				}
			}
		}
		if !not {
			return false
		}
	}
	return true
}

// testByTagMatch filters a given metric by matching a regular expression against
// the associated tags
func (q *TagQueryContext) testByTagMatch(def *idx.ArchiveInterned) bool {
	// special case for tag "name"
	if _, ok := q.tagMatch.missCache.Load("name"); !ok {
		if _, ok := q.tagMatch.matchCache.Load("name"); ok || q.tagMatch.Regex.MatchString("name") {
			if !ok {
				if atomic.LoadInt32(&q.tagMatch.matchCacheSize) < int32(matchCacheSize) {
					q.tagMatch.matchCache.Store("name", struct{}{})
					atomic.AddInt32(&q.tagMatch.matchCacheSize, 1)
				}
			}
			return true
		}
		if atomic.LoadInt32(&q.tagMatch.missCacheSize) < int32(matchCacheSize) {
			q.tagMatch.missCache.Store("name", struct{}{})
			atomic.AddInt32(&q.tagMatch.missCacheSize, 1)
		}
	}

	for _, tag := range def.Tags.KeyValues {
		if _, ok := q.tagMatch.missCache.Load(tag.Key); ok {
			continue
		}

		key, err := idx.IdxIntern.GetStringFromPtr(tag.Key)
		if err != nil {
			log.Error("memory-idx: Failed to retrieve uintptr for interned tag key: ", err)
			internError.Inc()
			continue
		}
		if _, ok := q.tagMatch.matchCache.Load(tag.Key); ok || q.tagMatch.Regex.MatchString(key) {
			if !ok {
				if atomic.LoadInt32(&q.tagMatch.matchCacheSize) < int32(matchCacheSize) {
					q.tagMatch.matchCache.Store(tag.Key, struct{}{})
					atomic.AddInt32(&q.tagMatch.matchCacheSize, 1)
				}
			}
			return true
		}
		if atomic.LoadInt32(&q.tagMatch.missCacheSize) < int32(matchCacheSize) {
			q.tagMatch.missCache.Store(tag.Key, struct{}{})
			atomic.AddInt32(&q.tagMatch.missCacheSize, 1)
		}
		continue
	}

	return false
}

// testByFrom filters a given metric by its LastUpdate time
func (q *TagQueryContext) testByFrom(def *idx.ArchiveInterned) bool {
	return q.from <= atomic.LoadInt64(&def.LastUpdate)
}

// testByPrefix filters a given metric by matching prefixes against the values
// of a specific tag
func (q *TagQueryContext) testByPrefix(def *idx.ArchiveInterned, exprs []kv) bool {
EXPRS:
	for _, e := range exprs {
		if e.Key == "name" && strings.HasPrefix(schema.SanitizeNameAsTagValue(def.Name.String()), e.Value) {
			continue EXPRS
		}

		for _, tag := range def.Tags.KeyValues {
			key, err := idx.IdxIntern.GetStringFromPtr(tag.Key)
			if err != nil {
				log.Error("memory-idx: Failed to retrieve uintptr for interned tag key: ", err)
				internError.Inc()
				continue
			}
			if e.Key != key {
				continue
			}
			value, err := idx.IdxIntern.GetStringFromPtr(tag.Value)
			if err != nil {
				log.Error("memory-idx: Failed to retrieve uintptr for interned tag value: ", err)
				internError.Inc()
				continue
			}
			if !strings.HasPrefix(value, e.Value) {
				continue
			}
			continue EXPRS
		}
		return false
	}
	return true
}

// testByTagPrefix filters a given metric by matching prefixes against its tags
func (q *TagQueryContext) testByTagPrefix(def *idx.ArchiveInterned) bool {
	if strings.HasPrefix("name", q.tagPrefix) {
		return true
	}

	for _, tag := range def.Tags.KeyValues {
		if strings.HasPrefix(tag.String(), q.tagPrefix) {
			return true
		}
	}

	return false
}

// testByEqual filters a given metric by the defined "=" expressions
func (q *TagQueryContext) testByEqual(id schema.MKey, exprs []kv, not bool) bool {
	for _, e := range exprs {
		key, err := idx.IdxIntern.GetPtrFromByte([]byte(e.Key))
		if err != nil {
			log.Error("memory-idx: Failed to retrieve uintptr for interned tag key: ", err)
			internError.Inc()
			continue
		}
		value, err := idx.IdxIntern.GetPtrFromByte([]byte(e.Value))
		if err != nil {
			log.Error("memory-idx: Failed to retrieve uintptr for interned tag value: ", err)
			internError.Inc()
			continue
		}
		indexIds := q.index[key][value]

		// shortcut if key=value combo does not exist at all
		if len(indexIds) == 0 {
			return not
		}

		if _, ok := indexIds[id]; ok {
			if not {
				return false
			}
		} else {
			if !not {
				return false
			}
		}
	}

	return true
}

// filterIdsFromChan takes a channel of metric ids and runs them through the
// required tests to decide whether a metric should be part of the final
// result set or not
// it returns the final result set via the given resCh parameter
func (q *TagQueryContext) filterIdsFromChan(idCh, resCh chan schema.MKey) {
	for id := range idCh {
		var def *idx.ArchiveInterned
		var ok bool

		if def, ok = q.byId[id]; !ok {
			// should never happen because every ID in the tag index
			// must be present in the byId lookup table
			corruptIndex.Inc()
			log.Errorf("memory-idx: ID %q is in tag index but not in the byId lookup table", id)
			continue
		}

		// we always omit tag filters because Run() does not support filtering by tags
		if q.testByAllExpressions(id, def, false) {
			resCh <- id
		}
	}

	q.wg.Done()
}

// sortByCost tries to estimate the cost of different expressions and sort them
// in increasing order
// this is to reduce the result set cheaply and only apply expensive tests to an
// already reduced set of results
func (q *TagQueryContext) sortByCost() {
	for i, kv := range q.equal {
		key, err := idx.IdxIntern.GetPtrFromByte([]byte(kv.Key))
		if err != nil {
			log.Error("memory-idx: Failed to retrieve uintptr for interned tag key: ", err)
			internError.Inc()
			continue
		}
		value, err := idx.IdxIntern.GetPtrFromByte([]byte(kv.Value))
		if err != nil {
			log.Error("memory-idx: Failed to retrieve uintptr for interned tag value: ", err)
			internError.Inc()
			continue
		}
		q.equal[i].cost = uint(len(q.index[key][value]))
	}

	// for prefix and match clauses we can't determine the actual cost
	// without actually evaluating them, so we estimate based on
	// cardinality of the key
	for i, kv := range q.prefix {
		key, err := idx.IdxIntern.GetPtrFromByte([]byte(kv.Key))
		if err != nil {
			log.Error("memory-idx: Failed to retrieve uintptr for interned tag key: ", err)
			internError.Inc()
			continue
		}
		q.prefix[i].cost = uint(len(q.index[key]))
	}

	for i, kvRe := range q.match {
		key, err := idx.IdxIntern.GetPtrFromByte([]byte(kvRe.Key))
		if err != nil {
			log.Error("memory-idx: Failed to retrieve uintptr for interned tag key: ", err)
			internError.Inc()
			continue
		}
		q.match[i].cost = uint(len(q.index[key]))
	}

	sort.Sort(KvByCost(q.equal))
	sort.Sort(KvByCost(q.notEqual))
	sort.Sort(KvByCost(q.prefix))
	sort.Sort(KvReByCost(q.match))
	sort.Sort(KvReByCost(q.notMatch))
}

// Run executes the tag query on the given index and returns a list of ids
func (q *TagQueryContext) Run(index TagIndex, byId map[schema.MKey]*idx.ArchiveInterned) IdSet {
	q.index = index
	q.byId = byId

	q.sortByCost()

	idCh, _ := q.getInitialIds()
	if idCh == nil {
		return make(IdSet)
	}
	resCh := make(chan schema.MKey)

	// start the tag query workers. they'll consume the ids on the idCh and
	// evaluate for each of them whether it satisfies all the conditions
	// defined in the query expressions. those that satisfy all conditions
	// will be pushed into the resCh
	q.wg.Add(TagQueryWorkers)
	for i := 0; i < TagQueryWorkers; i++ {
		go q.filterIdsFromChan(idCh, resCh)
	}

	go func() {
		q.wg.Wait()
		close(resCh)
	}()

	result := make(IdSet)

	for id := range resCh {
		result[id] = struct{}{}
	}

	return result
}

// getMaxTagCount calculates the maximum number of results (cardinality) a
// tag query could possibly return
// this is useful because when running a tag query we can abort it as soon as
// we know that there can't be more tags discovered and added to the result set
func (q *TagQueryContext) getMaxTagCount() int {
	defer q.wg.Done()
	var maxTagCount int

	if q.tagClause == tagquery.PREFIX_TAG && len(q.tagPrefix) > 0 {
		for tag := range q.index {
			key, err := idx.IdxIntern.GetStringFromPtr(tag)
			if err != nil {
				log.Error("memory-idx: Failed to retrieve uintptr for interned tag key: ", err)
				internError.Inc()
				continue
			}
			if !strings.HasPrefix(key, q.tagPrefix) {
				continue
			}
			maxTagCount++
		}
	} else if q.tagClause == tagquery.MATCH_TAG {
		for tag := range q.index {
			key, err := idx.IdxIntern.GetStringFromPtr(tag)
			if err != nil {
				log.Error("memory-idx: Failed to retrieve uintptr for interned tag key: ", err)
				internError.Inc()
				continue
			}
			if q.tagMatch.Regex.MatchString(key) {
				maxTagCount++
			}
		}
	} else {
		maxTagCount = len(q.index)
	}

	return maxTagCount
}

// filterTagsFromChan takes a channel of metric IDs and evaluates each of them
// according to the criteria associated with this query
// those that pass all the tests will have their relevant tags extracted, which
// are then pushed into the given tag channel
func (q *TagQueryContext) filterTagsFromChan(idCh chan schema.MKey, tagCh chan string, stopCh chan struct{}, omitTagFilters bool) {
	// used to prevent that this worker thread will push the same result into
	// the chan twice
	resultsCache := make(map[string]struct{})

IDS:
	for id := range idCh {
		var def *idx.ArchiveInterned
		var ok bool

		if def, ok = q.byId[id]; !ok {
			// should never happen because every ID in the tag index
			// must be present in the byId lookup table
			corruptIndex.Inc()
			log.Errorf("memory-idx: ID %q is in tag index but not in the byId lookup table", id)
			continue
		}

		// generate a set of all tags of the current metric that satisfy the
		// tag filter condition
		metricTags := make(map[string]struct{}, 0)
		for _, tag := range def.Tags.KeyValues {
			key, err := idx.IdxIntern.GetStringFromPtr(tag.Key)
			if err != nil {
				log.Error("memory-idx: Failed to retrieve uintptr for interned tag key: ", err)
				internError.Inc()
				continue
			}
			// this tag has already been pushed into tagCh, so we can stop evaluating
			if _, ok := resultsCache[key]; ok {
				continue
			}

			if q.tagClause == tagquery.PREFIX_TAG {
				if !strings.HasPrefix(key, q.tagPrefix) {
					continue
				}
			} else if q.tagClause == tagquery.MATCH_TAG {
				if _, ok := q.tagMatch.missCache.Load(tag.Key); ok || !q.tagMatch.Regex.MatchString(key) {
					if !ok {
						q.tagMatch.missCache.Store(tag.Key, struct{}{})
					}
					continue
				}
			}
			metricTags[key] = struct{}{}
		}

		// if we don't filter tags, then we can assume that "name" should always be part of the result set
		if omitTagFilters {
			if _, ok := resultsCache["name"]; !ok {
				metricTags["name"] = struct{}{}
			}
		}

		// if some tags satisfy the current tag filter condition then we run
		// the metric through all tag expression tests in order to decide
		// whether those tags should be part of the final result set
		if len(metricTags) > 0 {
			if q.testByAllExpressions(id, def, omitTagFilters) {
				for key := range metricTags {
					select {
					case tagCh <- key:
					case <-stopCh:
						// if execution of query has stopped because the max tag
						// count has been reached then tagCh <- might block
						// because that channel will not be consumed anymore. in
						// that case the stop channel will have been closed so
						// we so we exit here
						break IDS
					}
					resultsCache[key] = struct{}{}
				}
			} else {
				// check if we need to stop
				select {
				case <-stopCh:
					break IDS
				default:
				}
			}
		}
	}

	q.wg.Done()
}

// determines whether the given tag prefix/tag match will match the special
// tag "name". if it does, then we can omit some filtering because we know
// that every metric has a name
func (q *TagQueryContext) tagFilterMatchesName() bool {
	matchName := false

	if q.tagClause == tagquery.PREFIX_TAG || q.startWith == tagquery.PREFIX_TAG {
		if strings.HasPrefix("name", q.tagPrefix) {
			matchName = true
		}
	} else if q.tagClause == tagquery.MATCH_TAG || q.startWith == tagquery.MATCH_TAG {
		if q.tagMatch.Regex.MatchString("name") {
			matchName = true
		}
	} else {
		// some tag queries might have no prefix specified yet, in this case
		// we do not need to filter by the name
		// f.e. we know that every metric has a name, and we know that the
		// prefix "" matches the string "name", so we know that every metric
		// will pass the tag prefix test. hence we can omit the entire test.
		matchName = true
	}

	return matchName
}

// RunGetTags executes the tag query and returns all the tags of the
// resulting metrics
func (q *TagQueryContext) RunGetTags(index TagIndex, byId map[schema.MKey]*idx.ArchiveInterned) map[string]struct{} {
	q.index = index
	q.byId = byId

	maxTagCount := int32(math.MaxInt32)

	// start a thread to calculate the maximum possible number of tags.
	// this might not always complete before the query execution, but in most
	// cases it likely will. when it does end before the execution of the query,
	// the value of maxTagCount will be used to abort the query execution once
	// the max number of possible tags has been reached
	q.wg.Add(1)
	go atomic.StoreInt32(&maxTagCount, int32(q.getMaxTagCount()))

	q.sortByCost()
	idCh, stopCh := q.getInitialIds()
	tagCh := make(chan string)

	// we know there can only be 1 tag filter, so if we detect that the given
	// tag condition matches the special tag "name", we can omit the filtering
	// because every metric has a name.
	matchName := q.tagFilterMatchesName()

	// start the tag query workers. they'll consume the ids on the idCh and
	// evaluate for each of them whether it satisfies all the conditions
	// defined in the query expressions. then they will extract the tags of
	// those that satisfy all conditions and push them into tagCh.
	q.wg.Add(TagQueryWorkers)
	for i := 0; i < TagQueryWorkers; i++ {
		go q.filterTagsFromChan(idCh, tagCh, stopCh, matchName)
	}

	go func() {
		q.wg.Wait()
		close(tagCh)
	}()

	result := make(map[string]struct{})

	for tag := range tagCh {
		result[tag] = struct{}{}

		// if we know that there can't be more results than what we have
		// abort the query execution
		if int32(len(result)) >= atomic.LoadInt32(&maxTagCount) {
			break
		}
	}

	// abort query execution and wait for all workers to end
	close(stopCh)

	q.wg.Wait()
	return result
}
