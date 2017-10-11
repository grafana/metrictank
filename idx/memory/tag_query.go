package memory

import (
	"errors"
	"math"
	"regexp"

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

type kv struct {
	key   string
	value string
}

type expression struct {
	kv
	operator int
}

type kvRe struct {
	key   string
	value *regexp.Regexp
}

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
				e.value = "^" + e.value
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

func (q *TagQuery) getInitialByMatch(index TagIndex) (int, TagIDs) {
	resultSet := make(TagIDs)
	lowestCount := math.MaxUint32
	startId := 0

	// choose key that has the smallest number of values because we want to
	// reduce the number of matches and start with the smallest possible resultSet.
	// the smaller the resultSet the less time will have to be spent to further
	// filter its values in the following expressions.
	for i := range q.match {
		l := len(index[q.match[i].key])
		if l < lowestCount {
			lowestCount = l
			startId = i
		}
	}

	// shortcut if value == nil.
	// this will simply match any value, like ^.+. since we know that every value
	// in the index must not be empty, we can skip the matching.
	if q.match[startId].value == nil {
		for _, ids := range index[q.match[startId].key] {
			for id := range ids {
				resultSet[id] = struct{}{}
			}
		}
		return startId, resultSet
	}

	for v, ids := range index[q.match[startId].key] {
		if !q.match[startId].value.MatchString(v) {
			continue
		}

		for id := range ids {
			resultSet[id] = struct{}{}
		}
	}
	return startId, resultSet
}

func (q *TagQuery) getInitialByEqual(index TagIndex) (int, TagIDs) {
	resultSet := make(TagIDs)
	lowestCount := math.MaxUint32
	startId := 0

	// choose key-value combo that has the smallest number of series IDs because
	// we want to start with the smallest possible resultSet.
	// the smaller the resultSet the less time will have to be spent to further
	// filter its values in the following expressions.
	for i := range q.equal {
		l := len(index[q.equal[i].key][q.equal[i].value])
		if l < lowestCount {
			lowestCount = l
			startId = i
		}
	}

	// copy the map, because we'll later delete items from it
	for k, v := range index[q.equal[startId].key][q.equal[startId].value] {
		resultSet[k] = v
	}

	return startId, resultSet
}

// filterByEqual filters a list of metric ids by the given expressions. if "not"
// is true it will remove all ids of which at least one expression is equal to
// one of its tags. if "not" is false the functionality is inverted, so it
// removes all the ids where no tag is equal to at least one of the expressions.
//
// skipEqual:   an integer specifying an expression index that should be skipped
// resultSet:   list of series IDs that should be filtered
// index:       the tag index based on which to evaluate the conditions
// not:         whether the resultSet shall be filtered by a = or != condition
//
func (q *TagQuery) filterByEqual(skipEqual int, resultSet TagIDs, index TagIndex, not bool) {
	var expressions []kv
	if not {
		expressions = q.notEqual
	} else {
		expressions = q.equal
	}

	for i, e := range expressions {
		if i == skipEqual {
			continue
		}

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
// skipMatch:   an integer specifying an expression index that should be skipped
// resultSet:   list of series IDs that should be filtered
// byId:        ID keyed index of metric definitions, used to lookup the tags of IDs
// not:         whether the resultSet shall be filtered by the =~ or !=~ condition
//
func (q *TagQuery) filterByMatch(skipMatch int, resultSet TagIDs, byId map[string]*idx.Archive, not bool) {
	var expressions []kvRe
	if not {
		expressions = q.notMatch
	} else {
		expressions = q.match
	}

	for i, e := range expressions {
		if i == skipMatch {
			continue
		}

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
				CorruptIndex.Inc()
				log.Error(3, "memory-idx: ID %q is in tag index but not in the byId lookup table", id.String())
				delete(resultSet, id)
				continue IDS
			}

			for _, tag := range def.Tags {
				// reduce regex matching by looking up cached matches
				if _, ok := matchingTags[tag]; ok {
					if not {
						delete(resultSet, id)
					}
					continue IDS
				}
			}

			for _, tag := range def.Tags {
				// length of key doesn't match
				if len(tag) <= len(e.key)+1 || tag[len(e.key)] != 61 {
					continue
				}

				// reduce regex matching by looking up cached non-matches
				if _, ok := notMatchingTags[tag]; ok {
					continue
				}

				// value == nil means that this expression can be short cut
				// by not evaluating it
				if e.key == tag[:len(e.key)] && (e.value == nil || e.value.MatchString(tag[len(e.key)+1:])) {
					if len(matchingTags) < matchCacheSize {
						matchingTags[tag] = struct{}{}
					}
					if not {
						delete(resultSet, id)
					}
					continue IDS
				} else {
					if len(notMatchingTags) < matchCacheSize {
						notMatchingTags[tag] = struct{}{}
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
			CorruptIndex.Inc()
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
	var skipMatch, skipEqual = -1, -1
	var resultSet TagIDs

	if q.startWith == EQUAL {
		skipEqual, resultSet = q.getInitialByEqual(index)
	} else {
		skipMatch, resultSet = q.getInitialByMatch(index)
	}

	// filter the resultSet by the from condition and all other expressions given.
	// filters should be in ascending order by the cpu required to process them,
	// that way the most cpu intensive filters only get applied to the smallest
	// possible resultSet.
	q.filterByEqual(skipEqual, resultSet, index, false)
	q.filterByEqual(-1, resultSet, index, true)
	q.filterByFrom(resultSet, byId)
	q.filterByMatch(skipMatch, resultSet, byId, false)
	q.filterByMatch(-1, resultSet, byId, true)
	return resultSet
}
