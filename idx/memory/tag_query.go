package memory

import (
	"errors"
	"math"
	"regexp"
	"strings"

	"github.com/grafana/metrictank/idx"
)

var (
	expressionRe    = regexp.MustCompile("^([^!=~]+)([!=~]{1,3})(.*)$")
	errUnknownId    = errors.New("id does not exist")
	errInvalidTag   = errors.New("found an invalid tag in the index")
	errInvalidQuery = errors.New("invalid query")
)

type kv struct {
	key   string
	value string
}

const (
	PARSING_ERROR = iota
	EQUAL
	NOT_EQUAL
	MATCH
	NOT_MATCH
)

type expression struct {
	kv
	operator int
}

type TagQuery struct {
	equal    []kv
	match    []kv
	notEqual []kv
	notMatch []kv
}

// returns expression,
// in case of error the operator will be PARSING_ERROR
func parseExpression(expr string) expression {
	var key []byte
	regex, not := false, false
	var pos int

	// get key
	for ; pos < len(expr); pos++ {
		// ! || =
		if expr[pos] == 33 || expr[pos] == 61 {
			// key must not be empty
			if len(key) == 0 {
				return expression{operator: PARSING_ERROR}
			}
			break
		}

		// a-z A-Z 0-9
		if !(expr[pos] >= 97 && expr[pos] <= 122) && !(expr[pos] >= 65 && expr[pos] <= 90) && !(expr[pos] >= 48 && expr[pos] <= 57) {
			return expression{operator: PARSING_ERROR}
		}

		key = append(key, expr[pos])
	}

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

	var value []byte
	for ; pos < len(expr); pos++ {
		// enforce a-z A-Z 0-9 -_ if query is not regex
		if !regex && !(expr[pos] >= 97 && expr[pos] <= 122) && !(expr[pos] >= 65 && expr[pos] <= 90) && !(expr[pos] >= 48 && expr[pos] <= 57) && expr[pos] != 45 && expr[pos] != 95 {
			return expression{operator: PARSING_ERROR}
		}

		value = append(value, expr[pos])
	}

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

func NewTagQuery(expressions []string) (TagQuery, error) {
	q := TagQuery{}

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

func (q *TagQuery) getInitialByMatch(index TagIndex) (int, map[string]struct{}, error) {
	resultSet := make(map[string]struct{})
	lowestCount := math.MaxUint32
	startId := 0

	// choose key that has the smallest number of values
	for i := range q.match {
		l := len(index[q.match[i].key])
		if l < lowestCount {
			lowestCount = l
			startId = i
		}
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

func (q *TagQuery) getInitialByEqual(index TagIndex) (int, map[string]struct{}) {
	resultSet := make(map[string]struct{})
	lowestCount := math.MaxUint32
	startId := 0

	// choose key-value combo that has the smallest number of ids
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

func (q *TagQuery) filterByEqual(skipEqual int, resultSet map[string]struct{}, index TagIndex) {
	for i, e := range q.equal {
		if i == skipEqual {
			continue
		}

		for id := range resultSet {
			if _, ok := index[e.key][e.value][id]; !ok {
				delete(resultSet, id)
			}
		}
	}
}

func (q *TagQuery) filterByNotEqual(resultSet map[string]struct{}, index TagIndex, byId map[string]*idx.Archive) {
	for _, e := range q.notEqual {
		fullTag := e.key + "=" + e.value
	IDS:
		for id := range resultSet {
			var def *idx.Archive
			var ok bool
			if def, ok = byId[id]; !ok {
				// corrupt index
				delete(resultSet, id)
				continue IDS
			}
			for _, tag := range def.Tags {
				if tag == fullTag {
					delete(resultSet, id)
					continue IDS
				}
			}
		}
	}
}

func (q *TagQuery) filterByMatch(skipMatch int, resultSet map[string]struct{}, byId map[string]*idx.Archive) error {
	for i, e := range q.match {
		if i == skipMatch {
			continue
		}

		re, err := regexp.Compile(e.value)
		if err != nil {
			return errInvalidQuery
		}

		matchingTags := make(map[string]struct{})
	IDS:
		for id := range resultSet {
			var def *idx.Archive
			var ok bool
			if def, ok = byId[id]; !ok {
				// corrupt index
				delete(resultSet, id)
				continue IDS
			}
			for _, tag := range def.Tags {
				// optimization to reduce regex matching
				if _, ok := matchingTags[tag]; ok {
					continue IDS
				}
			}

			for _, tag := range def.Tags {
				tagSplits := strings.SplitN(tag, "=", 2)
				if len(tagSplits) != 2 {
					// corrupt index
					delete(resultSet, id)
					continue IDS
				}

				if e.key == tagSplits[0] && re.MatchString(tagSplits[1]) {
					matchingTags[tag] = struct{}{}
					continue IDS
				}
			}
			delete(resultSet, id)
		}
	}
	return nil
}

func (q *TagQuery) filterByNotMatch(skipMatch int, resultSet map[string]struct{}, byId map[string]*idx.Archive) error {
	for _, e := range q.notMatch {
		re, err := regexp.Compile(e.value)
		if err != nil {
			return errInvalidQuery
		}

		matchingTags := make(map[string]struct{})
	IDS:
		for id := range resultSet {
			var def *idx.Archive
			var ok bool
			if def, ok = byId[id]; !ok {
				// corrupt index
				delete(resultSet, id)
				continue IDS
			}

			// optimization to reduce regex matching
			for _, tag := range def.Tags {
				if _, ok := matchingTags[tag]; ok {
					delete(resultSet, id)
					continue IDS
				}
			}

			for _, tag := range def.Tags {
				tagSplits := strings.SplitN(tag, "=", 2)
				if len(tagSplits) != 2 {
					// corrupt index
					delete(resultSet, id)
					continue IDS
				}

				if e.key == tagSplits[0] && re.MatchString(tagSplits[1]) {
					matchingTags[tag] = struct{}{}
					delete(resultSet, id)
					continue IDS
				}
			}
		}
	}
	return nil
}

func (q *TagQuery) Run(index TagIndex, byId map[string]*idx.Archive) (map[string]struct{}, error) {
	var skipMatch, skipEqual = -1, -1
	var resultSet map[string]struct{}
	var err error

	// find the best expression to start with
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

	q.filterByEqual(skipEqual, resultSet, index)
	q.filterByNotEqual(resultSet, index, byId)
	err = q.filterByMatch(skipMatch, resultSet, byId)
	if err != nil {
		return nil, err
	}
	err = q.filterByNotMatch(skipMatch, resultSet, byId)
	if err != nil {
		return nil, err
	}

	return resultSet, nil
}
