package tagquery

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/grafana/metrictank/schema"
)

//go:generate msgp
type Tags []Tag

func ParseTags(tags []string) (Tags, error) {
	res := make(Tags, len(tags))
	for i := range tags {
		tag, err := ParseTag(tags[i])
		if err != nil {
			return nil, err
		}
		res[i] = tag
	}

	sort.Sort(res)

	return res, nil
}

func ParseTagsFromMetricName(name string) (Tags, error) {
	elements := strings.Split(name, ";")

	nameValue := schema.SanitizeNameAsTagValue(elements[0])

	if !schema.ValidateTagValue(nameValue) {
		return nil, fmt.Errorf("Metric name is invalid as tag value \"%s\"", nameValue)
	}

	if len(elements) < 2 {
		return []Tag{{Key: "name", Value: nameValue}}, nil
	}

	res, err := ParseTags(elements[1:])
	if err != nil {
		return nil, err
	}

	res = append(res, Tag{Key: "name", Value: nameValue})
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
	res := make([]string, len(t))
	for i := range t {
		t[i].StringIntoBuilder(&builder)
		res[i] = builder.String()
		builder.Reset()
	}
	return res
}

// MarshalJSON satisfies the json.Marshaler interface
// it is used by the api endpoint /metaTags to list the meta tag records
func (t Tags) MarshalJSON() ([]byte, error) {
	return json.Marshal(t.Strings())
}

func (t *Tags) UnmarshalJSON(data []byte) error {
	var tagStrings []string
	err := json.Unmarshal(data, &tagStrings)
	if err != nil {
		return err
	}

	parsed, err := ParseTags(tagStrings)
	if err != nil {
		return err
	}

	*t = parsed
	return nil
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
	if !schema.ValidateTagKey(res.Key) {
		return res, fmt.Errorf("Invalid tag key \"%s\"", res.Key)
	}

	res.Value = tag[equalPos+1:]
	if !schema.ValidateTagValue(res.Value) {
		return res, fmt.Errorf("Invalid tag value \"%s\"", res.Value)
	}

	return res, nil
}

func (t *Tag) StringIntoBuilder(builder *strings.Builder) {
	builder.WriteString(t.Key)
	builder.WriteString("=")
	builder.WriteString(t.Value)
}
