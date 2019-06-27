package tagQuery

import (
	"fmt"
	"sort"
	"strings"
)

type Tags []Tag

func ParseTags(tags []string) (Tags, error) {
	res := make(Tags, 0, len(tags))
	for i := range tags {
		tag, err := ParseTag(tags[i])
		if err != nil {
			return nil, err
		}
		res = append(res, tag)
	}

	sort.Sort(res)

	return res, nil
}

func ParseTagsFromMetricName(name string) (Tags, error) {
	elements := strings.Split(name, ";")

	if len(elements[0]) == 0 {
		return nil, fmt.Errorf("Metric name cannot have length 0")
	}

	// strip the ~ from the name, because it is not allowed in tag values
	for i := 0; i < len(elements[0]); {
		if elements[0][i] == '~' {
			elements[0] = elements[0][:i] + elements[0][i+1:]
		} else {
			i++
		}
	}

	// validate the name as a tag value
	err := validateValue(elements[0])
	if err != nil {
		return nil, err
	}

	if len(elements) < 2 {
		return []Tag{{Key: "name", Value: elements[0]}}, nil
	}

	res, err := ParseTags(elements[1:])
	if err != nil {
		return nil, err
	}

	res = append(res, Tag{Key: "name", Value: elements[0]})
	sort.Sort(res)

	return res, nil
}

func (t Tags) Len() int      { return len(t) }
func (t Tags) Swap(i, j int) { t[i], t[j] = t[j], t[i] }
func (t Tags) Less(i, j int) bool {
	if t[i].Key == t[j].Key {
		return t[i].Value < t[j].Value
	}
	return t[i].Key < t[j].Key
}

func (t Tags) Strings() []string {
	builder := strings.Builder{}
	res := make([]string, 0, len(t))
	for _, tag := range t {
		tag.StringIntoBuilder(&builder)
		res = append(res, builder.String())
		builder.Reset()
	}
	return res
}

type Tag struct {
	Key   string
	Value string
}

func ParseTag(tag string) (Tag, error) {
	res := Tag{}

	equalPos := strings.Index(tag, "=")
	if equalPos < 0 {
		return res, fmt.Errorf("Missing equal sign in tag: %s", tag)
	}

	res.Key = tag[:equalPos]
	err := validateKey(res.Key)
	if err != nil {
		return res, fmt.Errorf("Error when validating tag %s: %s", tag, err)
	}
	if len(res.Key) == 0 {
		return res, fmt.Errorf("Tag key may not be empty: %s", tag)
	}

	res.Value = tag[equalPos+1:]
	err = validateValue(res.Value)
	if err != nil {
		return res, fmt.Errorf("Error when validating tag %s: %s", tag, err)
	}
	if len(res.Value) == 0 {
		return res, fmt.Errorf("Tag value may not be empty: %s", tag)
	}

	return res, nil
}

func (t *Tag) StringIntoBuilder(builder *strings.Builder) {
	builder.WriteString(t.Key)
	builder.WriteString("=")
	builder.WriteString(t.Value)
}
