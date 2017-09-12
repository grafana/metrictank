package memory

import (
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/raintank/metrictank/idx"
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

func NewTagQuery(expressions []string) (*TagQuery, error) {
	if len(expressions) == 0 {
		return nil, errInvalidQuery
	}

	query := &TagQuery{}

	for _, expr := range expressions {
		if !expressionRe.MatchString(expr) {
			return nil, errInvalidQuery
		}

		exprSplits := expressionRe.FindAllStringSubmatch(expr, -1)
		if len(exprSplits[0]) != 4 {
			return nil, errInvalidQuery
		}

		key := exprSplits[0][1]
		operator := exprSplits[0][2]
		value := exprSplits[0][3]

		// special case of empty value
		if len(value) == 0 {
			if operator[0] == byte('=') {
				operator = "!=~"
				value = ".+"
			} else {
				operator = "=~"
				value = ".+"
			}
		}

		// always anchor all regular expressions at the beginning
		if operator[len(operator)-1] == byte('~') && len(value) > 0 && value[0] != byte('^') {
			value = fmt.Sprintf("^%s", value)
		}

		switch operator {
		case "=":
			query.selects = append(query.selects, func(idx TagIndex) (map[string]struct{}, error) {
				return idx[key][value], nil
			})
		case "=~":
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
		case "!=":
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
		case "!=~":
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
		return nil, errInvalidQuery
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
			for k, v := range res {
				intersect[k] = v
			}
			continue
		}

	INTERSECT:
		for id1 := range intersect {
			for id2 := range res {
				if id1 == id2 {
					continue INTERSECT
				}
			}
			delete(intersect, id1)
		}
	}

	for id := range intersect {
		var def *idx.Archive
		var ok bool
		if def, ok = byId[id]; !ok {
			return nil, errUnknownId
		}
		for _, f := range q.filters {
			keep, err := f(def)
			if err != nil {
				return nil, err
			}
			if !keep {
				delete(intersect, id)
			}
		}
	}

	return intersect, nil
}
