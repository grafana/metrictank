package memory

import (
	"errors"
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

type TagQuery struct {
	selects []func(TagIndex) (map[string]struct{}, error)
	filters []func(def *idx.Archive) (bool, error)
}

const (
	PARSING_ERROR = iota
	EQUAL
	NOT_EQUAL
	MATCH
	NOT_MATCH
)

// returns key, value, operator
// in case of error the operator will be PARSING_ERROR
func parseExpression(expr string) (string, string, int) {
	var key []byte
	regex, not := false, false
	var pos int

	// get key
	for ; pos < len(expr); pos++ {
		// ! || =
		if expr[pos] == 33 || expr[pos] == 61 {
			// key must not be empty
			if len(key) == 0 {
				return "", "", PARSING_ERROR
			}
			break
		}

		// a-z A-Z 0-9
		if !(expr[pos] >= 97 && expr[pos] <= 122) && !(expr[pos] >= 65 && expr[pos] <= 90) && !(expr[pos] >= 48 && expr[pos] <= 57) {
			return "", "", PARSING_ERROR
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
		return "", "", PARSING_ERROR
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
			return "", "", PARSING_ERROR
		}

		value = append(value, expr[pos])
	}

	if not {
		if regex {
			return string(key), string(value), NOT_MATCH
		} else {
			return string(key), string(value), NOT_EQUAL
		}
	} else {
		if regex {
			return string(key), string(value), MATCH
		} else {
			return string(key), string(value), EQUAL
		}
	}
}

func NewTagQuery(expressions []string) (TagQuery, error) {
	query := TagQuery{}

	if len(expressions) == 0 {
		return query, errInvalidQuery
	}

	for _, expr := range expressions {
		key, value, operator := parseExpression(expr)
		if operator == PARSING_ERROR {
			return query, errInvalidQuery
		}

		// special case of empty value
		if len(value) == 0 {
			if operator == EQUAL || operator == MATCH {
				operator = NOT_MATCH
				value = ".+"
			} else {
				operator = MATCH
				value = ".+"
			}
		}

		// always anchor all regular expressions at the beginning
		if (operator == MATCH || operator == NOT_MATCH) && len(value) > 0 && value[0] != byte('^') {
			value = "^" + value
		}

		switch operator {
		case EQUAL:
			query.selects = append(query.selects, func(idx TagIndex) (map[string]struct{}, error) {
				return idx[key][value], nil
			})
		case MATCH:
			query.selects = append(query.selects, func(idx TagIndex) (map[string]struct{}, error) {
				re, err := regexp.Compile(value)
				if err != nil {
					return nil, err
				}

				res := make(map[string]struct{})

				for v, ids := range idx[key] {
					if re.MatchString(v) {
						for id := range ids {
							res[id] = struct{}{}
						}
					}
				}

				return res, nil
			})
		case NOT_EQUAL:
			query.filters = append(query.filters,
				func(def *idx.Archive) (bool, error) {
					for _, tag := range def.Tags {
						tagSplits := strings.Split(tag, "=")
						if len(tagSplits) < 2 {
							return false, errInvalidTag
						}
						if tagSplits[0] == key && tagSplits[1] == value {
							return false, nil
						}
					}
					return true, nil
				},
			)
		case NOT_MATCH:
			query.filters = append(query.filters,
				func(def *idx.Archive) (bool, error) {
					re, err := regexp.Compile(value)
					if err != nil {
						return false, err
					}
					for _, tag := range def.Tags {
						tagSplits := strings.Split(tag, "=")
						if len(tagSplits) < 2 {
							return false, errInvalidTag
						}
						if tagSplits[0] == key && re.MatchString(tagSplits[1]) {
							return false, nil
						}
					}
					return true, nil
				},
			)
		}
	}

	if len(query.selects) == 0 {
		return query, errInvalidQuery
	}

	return query, nil
}

func (q *TagQuery) Run(index TagIndex, byId map[string]*idx.Archive) (map[string]struct{}, error) {
	if len(q.selects) == 0 {
		return nil, errInvalidQuery
	}

	intersect := make(map[string]struct{})
	for i, s := range q.selects {
		res, err := s(index)
		if err != nil {
			return nil, err
		}
		if i == 0 {
		BUILD_INITIAL:
			for k, v := range res {
				for _, f := range q.filters {
					var def *idx.Archive
					var ok bool
					if def, ok = byId[k]; !ok {
						return nil, errUnknownId
					}
					keep, err := f(def)
					if err != nil {
						return nil, err
					}
					if !keep {
						continue BUILD_INITIAL
					}
				}

				intersect[k] = v
			}
			continue
		}

		for id := range intersect {
			if _, ok := res[id]; ok {
				continue
			}
			delete(intersect, id)
		}
	}

	return intersect, nil
}
