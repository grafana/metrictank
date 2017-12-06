package memory

import (
	"errors"
	"regexp"
	"sort"
	"strings"

	"github.com/grafana/metrictank/idx"
	"github.com/raintank/worldping-api/pkg/log"
)

var (
	errInvalidQuery = errors.New("invalid query")
)

const (
	EQUAL = iota
	NOT_EQUAL
	MATCH
	MATCH_TAG
	NOT_MATCH
	PREFIX
	PREFIX_TAG
)

type expression struct {
	kv
	operator int
}

type kv struct {
	cost  uint // cost of evaluating expression, compared to other kv objects
	key   string
	value string
}

type kvRe struct {
	cost       uint // cost of evaluating expression, compared to other kvRe objects
	key        string
	value      *regexp.Regexp
	matchCache map[string]struct{}
	missCache  map[string]struct{}
}

type KvByCost []kv

func (a KvByCost) Len() int           { return len(a) }
func (a KvByCost) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a KvByCost) Less(i, j int) bool { return a[i].cost < a[j].cost }

type KvReByCost []kvRe

func (a KvReByCost) Len() int           { return len(a) }
func (a KvReByCost) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a KvReByCost) Less(i, j int) bool { return a[i].cost < a[j].cost }

type TagQuery struct {
	from      int64
	equal     []kv
	match     []kvRe
	notEqual  []kv
	notMatch  []kvRe
	prefix    []kv
	startWith int
	filterTag int

	// no need have more than one of tagMatch and tagPrefix
	tagMatch  kvRe
	tagPrefix string

	index TagIndex
	byId  map[string]*idx.Archive
}

func compileRe(pattern string) (*regexp.Regexp, error) {
	var re *regexp.Regexp
	var err error
	if pattern != "^.+" {
		re, err = regexp.Compile(pattern)
		if err != nil {
			return nil, err
		}
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
	for ; pos < len(expr); pos++ {
		// =
		if expr[pos] == 61 {
			break
		}

		// !
		if expr[pos] == 33 {
			not = true
			break
		}

		// ^
		if expr[pos] == 94 {
			prefix = true
			break
		}

		// disallow ; in key
		if expr[pos] == 59 {
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

	// expecting a =
	if len(expr) <= pos || expr[pos] != 61 {
		return res, errInvalidQuery
	}
	pos++

	// if ~
	if len(expr) > pos && expr[pos] == 126 {
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

	// special key to match on tag
	if res.key == "__tag" {
		// currently ! queries on tags are not supported
		// and values must be set
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
	q := TagQuery{from: from}

	if len(expressions) == 0 {
		return q, errInvalidQuery
	}

	for _, expr := range expressions {
		e, err := parseExpression(expr)
		if err != nil {
			return q, err
		}

		// special case of empty value
		if len(e.value) == 0 {
			if e.operator == EQUAL || e.operator == MATCH {
				q.notMatch = append(q.notMatch, kvRe{
					key:   e.key,
					value: nil,
				})
			} else {
				q.match = append(q.match, kvRe{
					key:   e.key,
					value: nil,
				})
			}
		} else {
			// always anchor all regular expressions at the beginning
			if (e.operator == MATCH || e.operator == NOT_MATCH || e.operator == MATCH_TAG) && e.value[0] != byte('^') {
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
				q.match = append(q.match, kvRe{key: e.key, value: re})
			case NOT_MATCH:
				re, err := compileRe(e.value)
				if err != nil {
					return q, errInvalidQuery
				}
				q.notMatch = append(q.notMatch, kvRe{key: e.key, value: re})
			case PREFIX:
				q.prefix = append(q.prefix, kv{key: e.key, value: e.value})
			case MATCH_TAG:
				re, err := compileRe(e.value)
				if err != nil {
					return q, errInvalidQuery
				}
				q.tagMatch = kvRe{value: re}

				// we only allow one query by tag
				if q.filterTag != 0 {
					return q, errInvalidQuery
				}
				q.filterTag = MATCH_TAG
			case PREFIX_TAG:
				q.tagPrefix = e.value

				// we only allow one query by tag
				if q.filterTag != 0 {
					return q, errInvalidQuery
				}
				q.filterTag = PREFIX_TAG
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
	} else if q.filterTag == PREFIX_TAG {
		q.startWith = PREFIX_TAG
	} else if q.filterTag == MATCH_TAG {
		q.startWith = MATCH_TAG
	} else {
		return q, errInvalidQuery
	}

	return q, nil
}

// getInitialByMatch returns the initial resultset by executing the given match expression
func (q *TagQuery) getInitialByMatch(expr kvRe) TagIDs {
	resultSet := make(TagIDs)

	// shortcut if value == nil.
	// this will simply match any value, like ^.+. since we know that every value
	// in the index must not be empty, we can skip the matching.
	if expr.value == nil {
		for _, ids := range q.index[expr.key] {
			for id := range ids {
				resultSet[id] = struct{}{}
			}
		}
		return resultSet
	}

	for v, ids := range q.index[expr.key] {
		if !expr.value.MatchString(v) {
			continue
		}

		for id := range ids {
			resultSet[id] = struct{}{}
		}
	}
	return resultSet
}

// getInitialByPrefix returns the initial resultset by executing the given prefix match expression
func (q *TagQuery) getInitialByPrefix(expr kv) TagIDs {
	resultSet := make(TagIDs)

	for v, ids := range q.index[expr.key] {
		if len(v) < len(expr.value) || v[:len(expr.value)] != expr.value {
			continue
		}

		for id := range ids {
			resultSet[id] = struct{}{}
		}
	}

	return resultSet
}

// getInitialByTagPrefix returns the initial resultset by creating a list of
// metric IDs of which at least one tag starts with the defined prefix
func (q *TagQuery) getInitialByTagPrefix() TagIDs {
	resultSet := make(TagIDs)

	for tag, values := range q.index {
		if len(tag) < len(q.tagPrefix) {
			continue
		}

		if tag[:len(q.tagPrefix)] != q.tagPrefix {
			continue
		}

		for _, ids := range values {
			for id := range ids {
				resultSet[id] = struct{}{}
			}
		}
	}

	return resultSet
}

// getInitialByTagMatch returns the initial resultset by creating a list of
// metric IDs of which at least one tag matches the defined regex
func (q *TagQuery) getInitialByTagMatch() TagIDs {
	resultSet := make(TagIDs)
	matchCache := make(map[string]struct{})

	for tag, values := range q.index {
		if _, ok := matchCache[tag]; ok || q.tagMatch.value.MatchString(tag) {
			if !ok {
				matchCache[tag] = struct{}{}
			}

			for _, ids := range values {
				for id := range ids {
					resultSet[id] = struct{}{}
				}
			}
		}
	}

	return resultSet
}

// getInitialByEqual returns the initial resultset by executing the given equal expression
func (q *TagQuery) getInitialByEqual(expr kv) TagIDs {
	resultSet := make(TagIDs)

	// copy the map, because we'll later delete items from it
	for k, v := range q.index[expr.key][expr.value] {
		resultSet[k] = v
	}

	return resultSet
}

func (q *TagQuery) testByAllExpressions(id idx.MetricID, def *idx.Archive) bool {
	if !q.testByFrom(def) {
		return false
	}

	if len(q.equal) > 0 && !q.testByEqual(id, q.equal, false) {
		return false
	}

	if len(q.notEqual) > 0 && !q.testByEqual(id, q.notEqual, true) {
		return false
	}

	if q.filterTag == PREFIX_TAG {
		if !q.testByTagPrefix(def) {
			return false
		}
	}

	if !q.testByPrefix(def, q.prefix) {
		return false
	}

	if q.filterTag == MATCH_TAG {
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

func (q *TagQuery) testByMatch(def *idx.Archive, exprs []kvRe, not bool) bool {
EXPRS:
	for _, e := range exprs {
		if e.key == "name" {
			if e.value.MatchString(def.Name) {
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
		for _, tag := range def.Tags {
			// length of key doesn't match
			if len(tag) <= len(e.key)+1 || tag[len(e.key)] != 61 {
				continue
			}

			if e.key != tag[:len(e.key)] {
				continue
			}

			value := tag[len(e.key)+1:]

			// reduce regex matching by looking up cached non-matches
			if _, ok := e.missCache[value]; ok {
				continue
			}

			// reduce regex matching by looking up cached matches
			if _, ok := e.matchCache[value]; ok {
				if not {
					return false
				}
				continue EXPRS
			}

			// value == nil means that this expression can be short cut
			// by not evaluating it
			if e.value == nil || e.value.MatchString(value) {
				if len(e.matchCache) < matchCacheSize {
					if e.matchCache == nil {
						e.matchCache = make(map[string]struct{})
					}
					e.matchCache[value] = struct{}{}
				}
				if not {
					return false
				}
				continue EXPRS
			} else {
				if len(e.missCache) < matchCacheSize {
					if e.missCache == nil {
						e.missCache = make(map[string]struct{})
					}
					e.missCache[value] = struct{}{}
				}
			}
		}
		if !not {
			return false
		}
	}
	return true
}

func (q *TagQuery) testByTagMatch(def *idx.Archive) bool {
	for _, tag := range def.Tags {
		equal := strings.Index(tag, "=")
		if equal < 0 {
			corruptIndex.Inc()
			log.Error(3, "memory-idx: tag is in index, but does not contain '=' sign: %s", tag)
			continue
		}
		key := tag[:equal]

		if _, ok := q.tagMatch.missCache[key]; ok {
			continue
		}

		if _, ok := q.tagMatch.matchCache[key]; ok || q.tagMatch.value.MatchString(key) {
			if !ok {
				if q.tagMatch.matchCache == nil {
					q.tagMatch.matchCache = make(map[string]struct{})
				}
				q.tagMatch.matchCache[key] = struct{}{}
			}
			return true
		} else {
			if _, ok := q.tagMatch.missCache[key]; !ok {
				if q.tagMatch.missCache == nil {
					q.tagMatch.missCache = make(map[string]struct{})
				}
				q.tagMatch.missCache[key] = struct{}{}
			}
			continue
		}
	}

	return false
}

func (q *TagQuery) testByFrom(def *idx.Archive) bool {
	return q.from <= def.LastUpdate
}

func (q *TagQuery) testByPrefix(def *idx.Archive, exprs []kv) bool {
EXPRS:
	for _, e := range exprs {
		for _, tag := range def.Tags {
			// continue if any of these match:
			// - length of tag is too short, so this can't be a match
			// - the position where we expect the = is not a =
			// - the key does not match
			// - the prefix value does not match
			if len(tag) < len(e.key)+len(e.value)+1 ||
				tag[len(e.key)] != 61 ||
				tag[:len(e.key)] != e.key ||
				tag[len(e.key)+1:len(e.key)+len(e.value)+1] != e.value {
				continue
			}
			continue EXPRS
		}
		return false
	}
	return true
}

func (q *TagQuery) testByTagPrefix(def *idx.Archive) bool {
	for _, tag := range def.Tags {
		if len(tag) < len(q.tagPrefix) {
			continue
		}

		if tag[:len(q.tagPrefix)] == q.tagPrefix {
			return true
		}
	}

	return false
}

func (q *TagQuery) testByEqual(id idx.MetricID, exprs []kv, not bool) bool {
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

func (q *TagQuery) sortByCost() {
	for i := range q.equal {
		q.equal[i].cost = uint(len(q.index[q.equal[i].key][q.equal[i].value]))
	}

	for i := range q.prefix {
		q.prefix[i].cost = uint(len(q.index[q.prefix[i].key][q.prefix[i].value]))
	}

	for i := range q.match {
		q.match[i].cost = uint(len(q.index[q.match[i].key]))
	}

	sort.Sort(KvByCost(q.equal))
	sort.Sort(KvByCost(q.notEqual))
	sort.Sort(KvByCost(q.prefix))
	sort.Sort(KvReByCost(q.match))
	sort.Sort(KvReByCost(q.notMatch))
}

func (q *TagQuery) getInitialResultSet() TagIDs {
	var resultSet TagIDs

	switch q.startWith {
	case EQUAL:
		resultSet = q.getInitialByEqual(q.equal[0])
		q.equal = q.equal[1:]
	case PREFIX:
		resultSet = q.getInitialByPrefix(q.prefix[0])
		q.prefix = q.prefix[1:]
	case PREFIX_TAG:
		resultSet = q.getInitialByTagPrefix()
	case MATCH_TAG:
		resultSet = q.getInitialByTagMatch()
	case MATCH:
		resultSet = q.getInitialByMatch(q.match[0])
		q.match = q.match[1:]
	}

	return resultSet
}

func (q *TagQuery) Run(index TagIndex, byId map[string]*idx.Archive) TagIDs {
	q.index = index
	q.byId = byId

	q.sortByCost()

	resultSet := q.getInitialResultSet()

	// filter the resultSet by the from condition and all other expressions given.
	// filters should be in ascending order by the cpu required to process them,
	// that way the most cpu intensive filters only get applied to the smallest
	// possible resultSet.
	for id := range resultSet {
		var def *idx.Archive
		var ok bool

		if def, ok = q.byId[id.String()]; !ok {
			// should never happen because every ID in the tag index
			// must be present in the byId lookup table
			corruptIndex.Inc()
			log.Error(3, "memory-idx: ID %q is in tag index but not in the byId lookup table", id.String())
			continue
		}

		if !q.testByAllExpressions(id, def) {
			delete(resultSet, id)
		}
	}

	return resultSet
}

func (q *TagQuery) RunGetTags(index TagIndex, byId map[string]*idx.Archive) map[string]struct{} {
	q.index = index
	q.byId = byId

	q.sortByCost()

	ids := q.getInitialResultSet()
	resultSet := make(map[string]struct{}, 0)
	missCache := make(map[string]struct{}, 0)

	for id := range ids {
		var def *idx.Archive
		var ok bool

		if def, ok = q.byId[id.String()]; !ok {
			// should never happen because every ID in the tag index
			// must be present in the byId lookup table
			corruptIndex.Inc()
			log.Error(3, "memory-idx: ID %q is in tag index but not in the byId lookup table", id.String())
			continue
		}

		metricTags := make(map[string]struct{}, 0)
		for _, tag := range def.Tags {
			equal := strings.Index(tag, "=")
			if equal < 0 {
				corruptIndex.Inc()
				log.Error(3, "memory-idx: tag is in index, but does not contain '=' sign: %s", tag)
				continue
			}

			key := tag[:equal]
			if q.filterTag == PREFIX_TAG {
				if len(key) < len(q.tagPrefix) {
					continue
				}
				if key[:len(q.tagPrefix)] != q.tagPrefix {
					continue
				}
			} else {
				if _, ok := missCache[key]; ok || !q.tagMatch.value.MatchString(tag) {
					if !ok {
						missCache[key] = struct{}{}
					}
					continue
				}
			}
			if _, ok := resultSet[key]; ok {
				continue
			}
			metricTags[key] = struct{}{}
		}

		if len(metricTags) > 0 {
			if q.testByAllExpressions(id, def) {
				for key := range metricTags {
					resultSet[key] = struct{}{}
				}
			}
		}
	}

	return resultSet
}
