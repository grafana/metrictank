package memory

import (
	"errors"
	"math"
	"regexp"
	"strings"

	"github.com/grafana/metrictank/idx"
)

var (
	errInvalidQuery = errors.New("invalid query")
)

const (
	PARSING_ERROR = iota
	EQUAL
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

type TagQuery struct {
	from     int64
	equal    []kv
	match    []kv
	notEqual []kv
	notMatch []kv
}

// returns expression,
// in case of error the operator will be PARSING_ERROR
func parseExpression(expr string) expression {
	var pos int
	regex, not := false, false

	// scan up to operator to get key
	for ; pos < len(expr); pos++ {
		// ! || =
		if expr[pos] == 33 || expr[pos] == 61 {
			// key must not be empty
			if pos == 0 {
				return expression{operator: PARSING_ERROR}
			}
			break
		}

		// disallow ; in key
		if expr[pos] == 59 {
			return expression{operator: PARSING_ERROR}
		}
	}

	key := expr[:pos]

	// if !
	if expr[pos] == 33 {
		not = true
		pos++
	}

	// expecting a =
	if expr[pos] != 61 {
		return expression{operator: PARSING_ERROR}
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
			return expression{operator: PARSING_ERROR}
		}
	}
	value := expr[valuePos:]

	if not {
		if regex {
			return expression{kv: kv{key: string(key), value: string(value)}, operator: NOT_MATCH}
		} else {
			return expression{kv: kv{key: string(key), value: string(value)}, operator: NOT_EQUAL}
		}
	} else {
		if regex {
			return expression{kv: kv{key: string(key), value: string(value)}, operator: MATCH}
		} else {
			return expression{kv: kv{key: string(key), value: string(value)}, operator: EQUAL}
		}
	}
}

func NewTagQuery(expressions []string, from int64) (TagQuery, error) {
	q := TagQuery{from: from}

	if len(expressions) == 0 {
		return q, errInvalidQuery
	}

	for _, expr := range expressions {
		e := parseExpression(expr)
		if e.operator == PARSING_ERROR {
			return q, errInvalidQuery
		}

		// special case of empty value
		if len(e.value) == 0 {
			if e.operator == EQUAL || e.operator == MATCH {
				q.notMatch = append(q.notMatch, kv{
					key:   e.key,
					value: "^.+",
				})
			} else {
				q.match = append(q.match, kv{
					key:   e.key,
					value: "^.+",
				})
			}
		} else {
			// always anchor all regular expressions at the beginning
			if (e.operator == MATCH || e.operator == NOT_MATCH) && len(e.value) > 0 && e.value[0] != byte('^') {
				e.value = "^" + e.value
			}

			switch e.operator {
			case EQUAL:
				q.equal = append(q.equal, e.kv)
			case NOT_EQUAL:
				q.notEqual = append(q.notEqual, e.kv)
			case MATCH:
				q.match = append(q.match, e.kv)
			case NOT_MATCH:
				q.notMatch = append(q.notMatch, e.kv)
			}
		}
	}

	return q, nil
}

func (q *TagQuery) getInitialByMatch(index TagIndex) (int, TagIDs, error) {
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

	// shortcut if pattern is ^.+ (f.e. expression "key!=~" will be translated to "key=~^.+")
	if q.match[startId].value == "^.+" {
		for _, ids := range index[q.match[startId].key] {
			for id := range ids {
				resultSet[id] = struct{}{}
			}
		}
		return startId, resultSet, nil
	}

	re, err := regexp.Compile(q.match[startId].value)
	if err != nil {
		return 0, nil, errInvalidQuery
	}

	for v, ids := range index[q.match[startId].key] {
		if !re.MatchString(v) {
			continue
		}

		for id := range ids {
			resultSet[id] = struct{}{}
		}
	}
	return startId, resultSet, nil
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

// filters a list of metric ids by the given expressions. if "not" is true it will
// remove all ids of which at least one expression is equal to one of its tags.
// if "not" is false the functionality is inverted, so it removes all the ids
// where no tag is equal to at least one of the expressions.
//
// expressions: the list of key & value pairs
// skipEqual:   an integer specifying an expression index that should be skipped
// resultSet:   list of series IDs that should be filtered
// index:       the tag index based on which to evaluate the conditions
// not:         whether the resultSet shall be filtered by a = or != condition
//
func (q *TagQuery) filterByEqual(expressions []kv, skipEqual int, resultSet TagIDs, index TagIndex, not bool) {
	for i, e := range expressions {
		if i == skipEqual {
			continue
		}

		for id := range resultSet {
			if _, ok := index[e.key][e.value][id]; ok == not {
				delete(resultSet, id)
			}
		}
	}
}

// filters a list of metric ids by the given expressions. it is assumed that the
// given expressions are all based on regular expressions.
//
// expressions: the list of key & value pairs
// skipMatch:   an integer specifying an expression index that should be skipped
// resultSet:   list of series IDs that should be filtered
// byId:        ID keyed index of metric definitions, used to lookup the tags of IDs
// not:         whether the resultSet shall be filtered by the =~ or !=~ condition
//
func (q *TagQuery) filterByMatch(expressions []kv, skipMatch int, resultSet TagIDs, byId map[string]*idx.Archive, not bool) error {
	for i, e := range expressions {
		if i == skipMatch {
			continue
		}

		var shortCut bool
		var re *regexp.Regexp
		var err error

		// shortcut if pattern is ^.+, any value will match so there's no need
		// to actually run the regular expression.
		// (f.e. expression "key!=" will be translated to "key=~^.+")
		if e.value == "^.+" {
			shortCut = true
		} else {
			re, err = regexp.Compile(e.value)
			if err != nil {
				return errInvalidQuery
			}
		}

		// cache all tags that have once matched the regular expression.
		// this is based on the assumption that many matching tags will be repeated
		// over multiple series, so there's no need to run the regex for each of them
		// because once we know that a tag matches we can just compare strings
		matchingTags := make(map[string]struct{})
	IDS:
		for id := range resultSet {
			var def *idx.Archive
			var ok bool
			if def, ok = byId[id.ToString()]; !ok {
				// should never happen because every ID in the tag index
				// must be present in the byId lookup table
				CorruptIndex.Inc()
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
				tagSplits := strings.SplitN(tag, "=", 2)
				if len(tagSplits) != 2 {
					// should never happen because every tag in the index
					// must have a valid format
					InvalidTagInIndex.Inc()
					delete(resultSet, id)
					continue IDS
				}

				if e.key == tagSplits[0] && (shortCut || re.MatchString(tagSplits[1])) {
					matchingTags[tag] = struct{}{}
					if not {
						delete(resultSet, id)
					}
					continue IDS
				}
			}
			if !not {
				delete(resultSet, id)
			}
		}
	}
	return nil
}

func (q *TagQuery) filterByFrom(resultSet TagIDs, byId map[string]*idx.Archive) {
	if q.from <= 0 {
		return
	}

	for id := range resultSet {
		var def *idx.Archive
		var ok bool
		if def, ok = byId[id.ToString()]; !ok {
			// should never happen because every ID in the tag index
			// must be present in the byId lookup table
			CorruptIndex.Inc()
			delete(resultSet, id)
			continue
		}

		if def.LastUpdate < q.from {
			delete(resultSet, id)
		}
	}
}

func (q *TagQuery) Run(index TagIndex, byId map[string]*idx.Archive) (TagIDs, error) {
	var skipMatch, skipEqual = -1, -1
	var resultSet TagIDs
	var err error

	// find the best expression to start with and retrieve its resultSet
	if len(q.equal) == 0 {
		if len(q.match) == 0 {
			return nil, errInvalidQuery
		}

		skipMatch, resultSet, err = q.getInitialByMatch(index)
		if err != nil {
			return nil, errInvalidQuery
		}
	} else {
		skipEqual, resultSet = q.getInitialByEqual(index)
	}

	// filter the resultSet by the from condition and all other expressions given.
	// filters should be in ascending order by the cpu required to process them,
	// that way the most cpu intensive filters only get applied to the smallest
	// possible resultSet.
	q.filterByFrom(resultSet, byId)
	q.filterByEqual(q.equal, skipEqual, resultSet, index, false)
	q.filterByEqual(q.notEqual, -1, resultSet, index, true)
	err = q.filterByMatch(q.match, skipMatch, resultSet, byId, false)
	if err != nil {
		return nil, err
	}
	err = q.filterByMatch(q.notMatch, -1, resultSet, byId, true)
	if err != nil {
		return nil, err
	}

	return resultSet, nil
}
