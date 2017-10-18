package memory

import (
	"errors"
	"regexp"
	"sort"

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
	NOT_MATCH
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
	cost  uint // cost of evaluating expression, compared to other kvRe objects
	key   string
	value *regexp.Regexp
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
	startWith int
}

// parseExpression returns an expression that's been generated from the given
// string, in case of error the operator will be PARSING_ERROR.
func parseExpression(expr string) (expression, error) {
	var pos int
	regex, not := false, false
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

	// shift over the ! character
	if not {
		pos++
	}

	// expecting a =
	if len(expr) <= pos || expr[pos] != 61 {
		return res, errInvalidQuery
	}
	pos++

	// if ~
	if len(expr) > pos && expr[pos] == 126 {
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

	if not {
		if regex {
			res.operator = NOT_MATCH
		} else {
			res.operator = NOT_EQUAL
		}
	} else {
		if regex {
			res.operator = MATCH
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
			if (e.operator == MATCH || e.operator == NOT_MATCH) && e.value[0] != byte('^') {
				e.value = "^(?:" + e.value + ")"
			}

			switch e.operator {
			case EQUAL:
				q.equal = append(q.equal, e.kv)
			case NOT_EQUAL:
				q.notEqual = append(q.notEqual, e.kv)
			case MATCH:
				var re *regexp.Regexp
				if e.value != "^.+" {
					re, err = regexp.Compile(e.value)
					if err != nil {
						return q, errInvalidQuery
					}
				}
				q.match = append(q.match, kvRe{key: e.key, value: re})
			case NOT_MATCH:
				var re *regexp.Regexp
				if e.value != "^.+" {
					re, err = regexp.Compile(e.value)
					if err != nil {
						return q, errInvalidQuery
					}
				}
				q.notMatch = append(q.notMatch, kvRe{key: e.key, value: re})
			}
		}
	}

	if len(q.equal) == 0 {
		if len(q.match) == 0 {
			return q, errInvalidQuery
		}
		q.startWith = MATCH
	} else {
		q.startWith = EQUAL
	}

	return q, nil
}

// getInitialByMatch returns the initial resultset by executing the given match expression
func (q *TagQuery) getInitialByMatch(index TagIndex, expr kvRe) TagIDs {
	resultSet := make(TagIDs)

	// shortcut if value == nil.
	// this will simply match any value, like ^.+. since we know that every value
	// in the index must not be empty, we can skip the matching.
	if expr.value == nil {
		for _, ids := range index[expr.key] {
			for id := range ids {
				resultSet[id] = struct{}{}
			}
		}
		return resultSet
	}

	for v, ids := range index[expr.key] {
		if !expr.value.MatchString(v) {
			continue
		}

		for id := range ids {
			resultSet[id] = struct{}{}
		}
	}
	return resultSet
}

// getInitialByEqual returns the initial resultset by executing the given equal expression
func (q *TagQuery) getInitialByEqual(index TagIndex, expr kv) TagIDs {
	resultSet := make(TagIDs)

	// copy the map, because we'll later delete items from it
	for k, v := range index[expr.key][expr.value] {
		resultSet[k] = v
	}

	return resultSet
}

// filterByEqual filters a list of metric ids by the given expressions. if "not"
// is true it will remove all ids of which at least one expression is equal to
// one of its tags. if "not" is false the functionality is inverted, so it
// removes all the ids where no tag is equal to at least one of the expressions.
//
// resultSet:   list of series IDs that should be filtered
// index:       the tag index based on which to evaluate the conditions
// not:         whether the resultSet shall be filtered by a = or != condition
//
func (q *TagQuery) filterByEqual(resultSet TagIDs, index TagIndex, exprs []kv, not bool) {

	for _, e := range exprs {
		indexIds := index[e.key][e.value]

		// shortcut if key=value combo does not exist at all
		if len(indexIds) == 0 && !not {
			for id := range resultSet {
				delete(resultSet, id)
			}
		}

		for id := range resultSet {
			if _, ok := indexIds[id]; ok == not {
				delete(resultSet, id)
			}
		}
	}
}

// filterByMatch filters a list of metric ids by the given expressions.
//
// resultSet:   list of series IDs that should be filtered
// byId:        ID keyed index of metric definitions, used to lookup the tags of IDs
// not:         whether the resultSet shall be filtered by the =~ or !=~ condition
//
func (q *TagQuery) filterByMatch(resultSet TagIDs, byId map[string]*idx.Archive, exprs []kvRe, not bool) {

	for _, e := range exprs {
		// cache all tags that have once matched the regular expression.
		// this is based on the assumption that many matching tags will be repeated
		// over multiple series, so there's no need to run the regex for each of them
		// because once we know that a tag matches we can just compare strings
		matchingTags := make(map[string]struct{})
		notMatchingTags := make(map[string]struct{})
	IDS:
		for id := range resultSet {
			var def *idx.Archive
			var ok bool
			if def, ok = byId[id.String()]; !ok {
				// should never happen because every ID in the tag index
				// must be present in the byId lookup table
				corruptIndex.Inc()
				log.Error(3, "memory-idx: ID %q is in tag index but not in the byId lookup table", id.String())
				delete(resultSet, id)
				continue IDS
			}

		TAGS:
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
				if _, ok := notMatchingTags[value]; ok {
					// each key should only be present once per `def`, so if
					// the key matches but the value doesn't we can skip the def
					break TAGS
				}

				// reduce regex matching by looking up cached matches
				if _, ok := matchingTags[value]; ok {
					if not {
						delete(resultSet, id)
					}
					continue IDS
				}

				// value == nil means that this expression can be short cut
				// by not evaluating it
				if e.value == nil || e.value.MatchString(value) {
					if len(matchingTags) < matchCacheSize {
						matchingTags[value] = struct{}{}
					}
					if not {
						delete(resultSet, id)
					}
					continue IDS
				} else {
					if len(notMatchingTags) < matchCacheSize {
						notMatchingTags[value] = struct{}{}
					}
				}
			}
			if !not {
				delete(resultSet, id)
			}
		}
	}
}

func (q *TagQuery) filterByFrom(resultSet TagIDs, byId map[string]*idx.Archive) {
	if q.from <= 0 {
		return
	}

	for id := range resultSet {
		var def *idx.Archive
		var ok bool
		if def, ok = byId[id.String()]; !ok {
			// should never happen because every ID in the tag index
			// must be present in the byId lookup table
			corruptIndex.Inc()
			log.Error(3, "memory-idx: ID %q is in tag index but not in the byId lookup table", id.String())
			delete(resultSet, id)
			continue
		}

		if def.LastUpdate < q.from {
			delete(resultSet, id)
		}
	}
}

func (q *TagQuery) Run(index TagIndex, byId map[string]*idx.Archive) TagIDs {
	var resultSet TagIDs

	for i := range q.equal {
		q.equal[i].cost = uint(len(index[q.equal[i].key][q.equal[i].value]))
	}

	for i := range q.match {
		q.match[i].cost = uint(len(index[q.match[i].key]))
	}

	sort.Sort(KvByCost(q.equal))
	sort.Sort(KvByCost(q.notEqual))
	sort.Sort(KvReByCost(q.match))
	sort.Sort(KvReByCost(q.notMatch))

	if q.startWith == EQUAL {
		resultSet = q.getInitialByEqual(index, q.equal[0])
		q.equal = q.equal[1:]
	} else {
		resultSet = q.getInitialByMatch(index, q.match[0])
		q.match = q.match[1:]
	}

	// filter the resultSet by the from condition and all other expressions given.
	// filters should be in ascending order by the cpu required to process them,
	// that way the most cpu intensive filters only get applied to the smallest
	// possible resultSet.
	q.filterByEqual(resultSet, index, q.equal, false)
	q.filterByEqual(resultSet, index, q.notEqual, true)
	q.filterByFrom(resultSet, byId)
	q.filterByMatch(resultSet, byId, q.match, false)
	q.filterByMatch(resultSet, byId, q.notMatch, true)
	return resultSet
}
