package memory

import (
	"errors"
	"math"
	"regexp"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/raintank/schema"

	"github.com/grafana/metrictank/idx"
	log "github.com/sirupsen/logrus"
)

var (
	errInvalidQuery = errors.New("invalid query")
)

// the supported operators are documented together with the graphite
// reference implementation:
// http://graphite.readthedocs.io/en/latest/tags.html
//
// some of the following operators are non-standard and are only used
// internally to implement certain functionalities requiring them

type match uint16

const (
	EQUAL      match = iota // =
	NOT_EQUAL               // !=
	MATCH                   // =~        regular expression
	MATCH_TAG               // __tag=~   relies on special key __tag. non-standard, required for `/metrics/tags` requests with "filter"
	NOT_MATCH               // !=~
	PREFIX                  // ^=        exact prefix, not regex. non-standard, required for auto complete of tag values
	PREFIX_TAG              // __tag^=   exact prefix with tag. non-standard, required for auto complete of tag keys
)

type expression struct {
	kv
	operator match
}

// a key / value combo used to represent a tag expression like "key=value"
// the cost is an estimate how expensive this query is compared to others
// with the same operator
type kv struct {
	cost  uint // cost of evaluating expression, compared to other kv objects
	key   string
	value string
}

func (k *kv) stringIntoBuilder(builder *strings.Builder) {
	builder.WriteString(k.key)
	builder.WriteString("=")
	builder.WriteString(k.value)
}

// kv expressions that rely on regular expressions will get converted to kvRe in
// NewTagQuery() to accommodate the additional requirements of regex based queries.
type kvRe struct {
	cost           uint // cost of evaluating expression, compared to other kvRe objects
	key            string
	value          *regexp.Regexp // the regexp pattern to evaluate, nil means everything should match
	matchCache     *sync.Map      // needs to be reference so kvRe can be copied, caches regex matches
	matchCacheSize int32          // sync.Map does not have a way to get the length
	missCache      *sync.Map      // needs to be reference so kvRe can be copied, caches regex misses
	missCacheSize  int32          // sync.Map does not have a way to get the length
}

type KvByCost []kv

func (a KvByCost) Len() int           { return len(a) }
func (a KvByCost) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a KvByCost) Less(i, j int) bool { return a[i].cost < a[j].cost }

type KvReByCost []kvRe

func (a KvReByCost) Len() int           { return len(a) }
func (a KvReByCost) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a KvReByCost) Less(i, j int) bool { return a[i].cost < a[j].cost }

// TagQuery runs a set of pattern or string matches on tag keys and values against
// the index. It is executed via:
// Run() which returns a set of matching MetricIDs
// RunGetTags() which returns a list of tags of the matching metrics
type TagQuery struct {
	// clause that operates on LastUpdate field
	from int64

	metricExpressions []expression
	mixedExpressions  []expression
	tagQuery          expression
	initialExpression expression

	// clauses that operate on values. from expressions like tag<operator>value
	equal    []kv   // EQUAL
	match    []kvRe // MATCH
	notEqual []kv   // NOT_EQUAL
	notMatch []kvRe // NOT_MATCH
	prefix   []kv   // PREFIX

	// clause that operate on tags (keys)
	// we only need to support 1 condition for now: a prefix or match
	tagClause match  // to know the clause type. either PREFIX_TAG or MATCH_TAG (or 0 if unset)
	tagMatch  kvRe   // only used for /metrics/tags with regex in filter param
	tagPrefix string // only used for auto complete of tags to match exact prefix

	startWith match // choses the first clause to generate the initial result set (one of EQUAL PREFIX MATCH MATCH_TAG PREFIX_TAG)

	index TagIndex                     // the tag index, hierarchy of tags & values, set by Run()/RunGetTags()
	byId  map[schema.MKey]*idx.Archive // the metric index by ID, set by Run()/RunGetTags()

	wg *sync.WaitGroup
}

func compileRe(pattern string) (*regexp.Regexp, error) {
	// shortcut, we don't need to compile that pattern, if re == nil we'll
	// simply check if there is any value and save a regex match
	if pattern == "^.+" {
		return nil, nil
	}

	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, err
	}

	return re, nil
}

// parseExpression returns an expression that's been generated from the given
// string, in case of error the operator will be PARSING_ERROR.
func parseExpression(expr string) (expression, error) {
	var pos int
	prefix, regex, not := false, false, false
	res := expression{}

	// scan up to operator to get key
FIND_OPERATOR:
	for ; pos < len(expr); pos++ {
		switch expr[pos] {
		case '=':
			break FIND_OPERATOR
		case '!':
			not = true
			break FIND_OPERATOR
		case '^':
			prefix = true
			break FIND_OPERATOR
		case ';':
			return res, errInvalidQuery
		}
	}

	// key must not be empty
	if pos == 0 {
		return res, errInvalidQuery
	}

	res.key = expr[:pos]

	// shift over the !/^ characters
	if not || prefix {
		pos++
	}

	if len(expr) <= pos || expr[pos] != '=' {
		return res, errInvalidQuery
	}
	pos++

	if len(expr) > pos && expr[pos] == '~' {
		// ^=~ is not a valid operator
		if prefix {
			return res, errInvalidQuery
		}
		regex = true
		pos++
	}

	valuePos := pos
	for ; pos < len(expr); pos++ {
		// disallow ; in value
		if expr[pos] == 59 {
			return res, errInvalidQuery
		}
	}
	res.value = expr[valuePos:]

	// special key to match on tag instead of a value
	if res.key == "__tag" {
		// currently ! (not) queries on tags are not supported
		// and unlike normal queries a value must be set
		if not || len(res.value) == 0 {
			return res, errInvalidQuery
		}

		if regex {
			res.operator = MATCH_TAG
		} else if prefix {
			res.operator = PREFIX_TAG
		} else {
			// currently only match & prefix operator are supported on tag
			return res, errInvalidQuery
		}

		return res, nil
	}

	if not {
		if regex {
			res.operator = NOT_MATCH
		} else {
			res.operator = NOT_EQUAL
		}
	} else {
		if regex {
			res.operator = MATCH
		} else if prefix {
			res.operator = PREFIX
		} else {
			res.operator = EQUAL
		}
	}
	return res, nil
}

func NewTagQuery(expressions []string, from int64) (TagQuery, error) {
	q := TagQuery{from: from, wg: &sync.WaitGroup{}}

	if len(expressions) == 0 {
		return q, errInvalidQuery
	}

	sort.Strings(expressions)
	for i, expr := range expressions {
		// skip duplicate expression
		if i > 0 && expr == expressions[i-1] {
			continue
		}

		e, err := parseExpression(expr)
		if err != nil {
			return q, err
		}

		// special case of empty value
		if len(e.value) == 0 {
			expression := kvRe{
				key:        e.key,
				value:      nil,
				matchCache: &sync.Map{},
				missCache:  &sync.Map{},
			}
			if e.operator == EQUAL || e.operator == MATCH {
				q.notMatch = append(q.notMatch, expression)
			} else {
				q.match = append(q.match, expression)
			}
		} else {
			// always anchor all regular expressions at the beginning if they do not start with ^
			if (e.operator == MATCH || e.operator == NOT_MATCH || e.operator == MATCH_TAG) && e.value[0] != '^' {
				e.value = "^(?:" + e.value + ")"
			}

			switch e.operator {
			case EQUAL:
				q.equal = append(q.equal, e.kv)
			case NOT_EQUAL:
				q.notEqual = append(q.notEqual, e.kv)
			case MATCH:
				re, err := compileRe(e.value)
				if err != nil {
					return q, errInvalidQuery
				}
				q.match = append(q.match, kvRe{
					key:        e.key,
					value:      re,
					matchCache: &sync.Map{},
					missCache:  &sync.Map{},
				})
			case NOT_MATCH:
				re, err := compileRe(e.value)
				if err != nil {
					return q, errInvalidQuery
				}
				q.notMatch = append(q.notMatch, kvRe{
					key:        e.key,
					value:      re,
					matchCache: &sync.Map{},
					missCache:  &sync.Map{},
				})
			case PREFIX:
				q.prefix = append(q.prefix, kv{key: e.key, value: e.value})
			case MATCH_TAG:
				// we only allow one query by tag
				if q.tagClause != 0 {
					return q, errInvalidQuery
				}

				re, err := compileRe(e.value)
				if err != nil {
					return q, errInvalidQuery
				}

				q.tagMatch = kvRe{
					value:      re,
					matchCache: &sync.Map{},
					missCache:  &sync.Map{},
				}

				q.tagClause = MATCH_TAG
			case PREFIX_TAG:
				// we only allow one query by tag
				if q.tagClause != 0 {
					return q, errInvalidQuery
				}

				q.tagPrefix = e.value
				q.tagClause = PREFIX_TAG
			}
		}
	}

	// the cheapest operator to minimize the result set should have precedence
	if len(q.equal) > 0 {
		q.startWith = EQUAL
	} else if len(q.prefix) > 0 {
		q.startWith = PREFIX
	} else if len(q.match) > 0 {
		q.startWith = MATCH
	} else if q.tagClause == PREFIX_TAG {
		// starting with a tag based query can be very expensive because they
		// have the potential to result in a huge initial result set
		q.startWith = PREFIX_TAG
	} else if q.tagClause == MATCH_TAG {
		q.startWith = MATCH_TAG
	} else {
		return q, errInvalidQuery
	}

	return q, nil
}

// getInitialByEqual generates the initial resultset by executing the given equal expression
func (q *TagQuery) getInitialByEqual(expr kv, idCh chan schema.MKey, stopCh chan struct{}) {
	defer q.wg.Done()

KEYS:
	for k := range q.index[expr.key][expr.value] {
		select {
		case <-stopCh:
			break KEYS
		case idCh <- k:
		}
	}

	close(idCh)
}

// getInitialByPrefix generates the initial resultset by executing the given prefix match expression
func (q *TagQuery) getInitialByPrefix(expr kv, idCh chan schema.MKey, stopCh chan struct{}) {
	defer q.wg.Done()

VALUES:
	for v, ids := range q.index[expr.key] {
		if !strings.HasPrefix(v, expr.value) {
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
func (q *TagQuery) getInitialByMatch(expr kvRe, idCh chan schema.MKey, stopCh chan struct{}) {
	defer q.wg.Done()

	// shortcut if value == nil.
	// this will simply match any value, like ^.+. since we know that every value
	// in the index must not be empty, we can skip the matching.
	if expr.value == nil {
	VALUES1:
		for _, ids := range q.index[expr.key] {
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
	for v, ids := range q.index[expr.key] {
		if !expr.value.MatchString(v) {
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
func (q *TagQuery) getInitialByTagPrefix(idCh chan schema.MKey, stopCh chan struct{}) {
	defer q.wg.Done()

TAGS:
	for tag, values := range q.index {
		if !strings.HasPrefix(tag, q.tagPrefix) {
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
func (q *TagQuery) getInitialByTagMatch(idCh chan schema.MKey, stopCh chan struct{}) {
	defer q.wg.Done()

TAGS:
	for tag, values := range q.index {
		if q.tagMatch.value.MatchString(tag) {
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
func (q *TagQuery) getInitialIds() (chan schema.MKey, chan struct{}) {
	idCh := make(chan schema.MKey, 1000)
	stopCh := make(chan struct{})
	q.wg.Add(1)

	switch q.startWith {
	case EQUAL:
		query := q.equal[0]
		q.equal = q.equal[1:]
		go q.getInitialByEqual(query, idCh, stopCh)
	case PREFIX:
		query := q.prefix[0]
		q.prefix = q.prefix[1:]
		go q.getInitialByPrefix(query, idCh, stopCh)
	case MATCH:
		query := q.match[0]
		q.match = q.match[1:]
		go q.getInitialByMatch(query, idCh, stopCh)
	case PREFIX_TAG:
		go q.getInitialByTagPrefix(idCh, stopCh)
	case MATCH_TAG:
		go q.getInitialByTagMatch(idCh, stopCh)
	}

	return idCh, stopCh
}

// testByAllExpressions takes and id and a MetricDefinition and runs it through
// all required tests in order to decide whether this metric should be part
// of the final result set or not
// in map/reduce terms this is the reduce function
func (q *TagQuery) testByAllExpressions(id schema.MKey, def *idx.Archive, omitTagFilters bool) bool {
	if !q.testByFrom(def) {
		return false
	}

	if len(q.equal) > 0 && !q.testByEqual(id, q.equal, false) {
		return false
	}

	if len(q.notEqual) > 0 && !q.testByEqual(id, q.notEqual, true) {
		return false
	}

	if q.tagClause == PREFIX_TAG && !omitTagFilters && q.startWith != PREFIX_TAG {
		if !q.testByTagPrefix(def) {
			return false
		}
	}

	if !q.testByPrefix(def, q.prefix) {
		return false
	}

	if q.tagClause == MATCH_TAG && !omitTagFilters && q.startWith != MATCH_TAG {
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
func (q *TagQuery) testByMatch(def *idx.Archive, exprs []kvRe, not bool) bool {
EXPRS:
	for _, e := range exprs {
		if e.key == "name" {
			if e.value == nil || e.value.MatchString(def.Name) {
				if not {
					return false
				} else {
					continue EXPRS
				}
			} else {
				if !not {
					return false
				} else {
					continue EXPRS
				}
			}
		}

		prefix := e.key + "="
		for _, tag := range def.Tags {
			if !strings.HasPrefix(tag, prefix) {
				continue
			}

			value := tag[len(e.key)+1:]

			// reduce regex matching by looking up cached non-matches
			if _, ok := e.missCache.Load(value); ok {
				continue
			}

			// reduce regex matching by looking up cached matches
			if _, ok := e.matchCache.Load(value); ok {
				if not {
					return false
				}
				continue EXPRS
			}

			// value == nil means that this expression can be short cut
			// by not evaluating it
			if e.value == nil || e.value.MatchString(value) {
				if atomic.LoadInt32(&e.matchCacheSize) < int32(matchCacheSize) {
					e.matchCache.Store(value, struct{}{})
					atomic.AddInt32(&e.matchCacheSize, 1)
				}
				if not {
					return false
				}
				continue EXPRS
			} else {
				if atomic.LoadInt32(&e.missCacheSize) < int32(matchCacheSize) {
					e.missCache.Store(value, struct{}{})
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
func (q *TagQuery) testByTagMatch(def *idx.Archive) bool {
	// special case for tag "name"
	if _, ok := q.tagMatch.missCache.Load("name"); !ok {
		if _, ok := q.tagMatch.matchCache.Load("name"); ok || q.tagMatch.value.MatchString("name") {
			if !ok {
				if atomic.LoadInt32(&q.tagMatch.matchCacheSize) < int32(matchCacheSize) {
					q.tagMatch.matchCache.Store("name", struct{}{})
					atomic.AddInt32(&q.tagMatch.matchCacheSize, 1)
				}
			}
			return true
		} else {
			if atomic.LoadInt32(&q.tagMatch.missCacheSize) < int32(matchCacheSize) {
				q.tagMatch.missCache.Store("name", struct{}{})
				atomic.AddInt32(&q.tagMatch.missCacheSize, 1)
			}
		}
	}

	for _, tag := range def.Tags {
		equal := strings.Index(tag, "=")
		if equal < 0 {
			corruptIndex.Inc()
			log.Errorf("memory-idx: ID %q has tag %q in index without '=' sign", def.Id, tag)
			continue
		}
		key := tag[:equal]

		if _, ok := q.tagMatch.missCache.Load(key); ok {
			continue
		}

		if _, ok := q.tagMatch.matchCache.Load(key); ok || q.tagMatch.value.MatchString(key) {
			if !ok {
				if atomic.LoadInt32(&q.tagMatch.matchCacheSize) < int32(matchCacheSize) {
					q.tagMatch.matchCache.Store(key, struct{}{})
					atomic.AddInt32(&q.tagMatch.matchCacheSize, 1)
				}
			}
			return true
		} else {
			if atomic.LoadInt32(&q.tagMatch.missCacheSize) < int32(matchCacheSize) {
				q.tagMatch.missCache.Store(key, struct{}{})
				atomic.AddInt32(&q.tagMatch.missCacheSize, 1)
			}
			continue
		}
	}

	return false
}

// testByFrom filters a given metric by its LastUpdate time
func (q *TagQuery) testByFrom(def *idx.Archive) bool {
	return q.from <= atomic.LoadInt64(&def.LastUpdate)
}

// testByPrefix filters a given metric by matching prefixes against the values
// of a specific tag
func (q *TagQuery) testByPrefix(def *idx.Archive, exprs []kv) bool {
EXPRS:
	for _, e := range exprs {
		if e.key == "name" && strings.HasPrefix(def.Name, e.value) {
			continue EXPRS
		}

		prefix := e.key + "=" + e.value
		for _, tag := range def.Tags {
			if !strings.HasPrefix(tag, prefix) {
				continue
			}
			continue EXPRS
		}
		return false
	}
	return true
}

// testByTagPrefix filters a given metric by matching prefixes against its tags
func (q *TagQuery) testByTagPrefix(def *idx.Archive) bool {
	if strings.HasPrefix("name", q.tagPrefix) {
		return true
	}

	for _, tag := range def.Tags {
		if strings.HasPrefix(tag, q.tagPrefix) {
			return true
		}
	}

	return false
}

// testByEqual filters a given metric by the defined "=" expressions
func (q *TagQuery) testByEqual(id schema.MKey, exprs []kv, not bool) bool {
	for _, e := range exprs {
		indexIds := q.index[e.key][e.value]

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
func (q *TagQuery) filterIdsFromChan(idCh, resCh chan schema.MKey) {
	for id := range idCh {
		var def *idx.Archive
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
func (q *TagQuery) sortByCost() {
	for i, kv := range q.equal {
		q.equal[i].cost = uint(len(q.index[kv.key][kv.value]))
	}

	// for prefix and match clauses we can't determine the actual cost
	// without actually evaluating them, so we estimate based on
	// cardinality of the key
	for i, kv := range q.prefix {
		q.prefix[i].cost = uint(len(q.index[kv.key]))
	}

	for i, kvRe := range q.match {
		q.match[i].cost = uint(len(q.index[kvRe.key]))
	}

	sort.Sort(KvByCost(q.equal))
	sort.Sort(KvByCost(q.notEqual))
	sort.Sort(KvByCost(q.prefix))
	sort.Sort(KvReByCost(q.match))
	sort.Sort(KvReByCost(q.notMatch))
}

// Run executes the tag query on the given index and returns a list of ids
func (q *TagQuery) Run(index TagIndex, byId map[schema.MKey]*idx.Archive) IdSet {
	q.index = index
	q.byId = byId

	q.sortByCost()

	idCh, _ := q.getInitialIds()
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
func (q *TagQuery) getMaxTagCount() int {
	defer q.wg.Done()
	var maxTagCount int

	if q.tagClause == PREFIX_TAG && len(q.tagPrefix) > 0 {
		for tag := range q.index {
			if !strings.HasPrefix(tag, q.tagPrefix) {
				continue
			}
			maxTagCount++
		}
	} else if q.tagClause == MATCH_TAG {
		for tag := range q.index {
			if q.tagMatch.value.MatchString(tag) {
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
func (q *TagQuery) filterTagsFromChan(idCh chan schema.MKey, tagCh chan string, stopCh chan struct{}, omitTagFilters bool) {
	// used to prevent that this worker thread will push the same result into
	// the chan twice
	resultsCache := make(map[string]struct{})

IDS:
	for id := range idCh {
		var def *idx.Archive
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
		for _, tag := range def.Tags {
			equal := strings.Index(tag, "=")
			if equal < 0 {
				corruptIndex.Inc()
				log.Errorf("memory-idx: ID %q has tag %q in index without '=' sign", id, tag)
				continue
			}

			key := tag[:equal]
			// this tag has already been pushed into tagCh, so we can stop evaluating
			if _, ok := resultsCache[key]; ok {
				continue
			}

			if q.tagClause == PREFIX_TAG {
				if !strings.HasPrefix(key, q.tagPrefix) {
					continue
				}
			} else if q.tagClause == MATCH_TAG {
				if _, ok := q.tagMatch.missCache.Load(key); ok || !q.tagMatch.value.MatchString(tag) {
					if !ok {
						q.tagMatch.missCache.Store(key, struct{}{})
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
func (q *TagQuery) tagFilterMatchesName() bool {
	matchName := false

	if q.tagClause == PREFIX_TAG || q.startWith == PREFIX_TAG {
		if strings.HasPrefix("name", q.tagPrefix) {
			matchName = true
		}
	} else if q.tagClause == MATCH_TAG || q.startWith == MATCH_TAG {
		if q.tagMatch.value.MatchString("name") {
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
func (q *TagQuery) RunGetTags(index TagIndex, byId map[schema.MKey]*idx.Archive) map[string]struct{} {
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
