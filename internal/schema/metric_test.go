package schema

import (
	"reflect"
	"sort"
	"testing"
	"unsafe"
)

func BenchmarkSetId(b *testing.B) {
	metric := MetricData{
		OrgId:    1234,
		Name:     "key1=val1.key2=val2.my.test.metric.name",
		Interval: 15,
		Value:    0.1234,
		Unit:     "ms",
		Time:     1234567890,
		Mtype:    "gauge",
		Tags:     []string{"key1:val1", "key2:val2"},
	}
	for i := 0; i < b.N; i++ {
		metric.SetId()
	}
}

func TestValidate(t *testing.T) {
	type testCase struct {
		md        MetricData
		expecting error
	}

	md := MetricData{
		Id:       "1.12345678901234567890123456789012",
		OrgId:    1,
		Name:     "abc",
		Interval: 1,
		Value:    2,
		Time:     3,
		Mtype:    "gauge",
	}

	mdZeroOrgId := md
	mdZeroOrgId.OrgId = 0

	mdZeroInterval := md
	mdZeroInterval.Interval = 0

	mdEmptyName := md
	mdEmptyName.Name = ""

	mdInvalidMType := md
	mdInvalidMType.Mtype = "somejunk"

	mdInvalidUtf8Name := md
	mdInvalidUtf8Name.Name = string("a\xc5")

	mdInvalidUtf8InTag := md
	tags := []string{"abc=de\xc5"}
	mdInvalidUtf8InTag.Tags = tags

	mdInvalidUtf8InTagAndInvalidTag := md
	tags = []string{"ab\xc5=@@!#@;;"}
	mdInvalidUtf8InTagAndInvalidTag.Tags = tags

	testCases := []testCase{
		{md, nil},
		{mdZeroOrgId, ErrInvalidOrgIdzero},
		{mdZeroInterval, ErrInvalidIntervalzero},
		{mdEmptyName, ErrInvalidEmptyName},
		{mdInvalidMType, ErrInvalidMtype},
		{mdInvalidUtf8Name, ErrInvalidInput},
		{mdInvalidUtf8InTag, ErrInvalidInput},
		{mdInvalidUtf8InTagAndInvalidTag, ErrInvalidInput},
	}

	for _, tc := range testCases {
		err := tc.md.Validate()
		if tc.expecting != err {
			t.Fatalf("Expected %t, but testcase %v returned %v", tc.expecting, tc.md, err)
		}
	}
}

func TestTagValidation(t *testing.T) {
	type testCase struct {
		tag       []string
		expecting error
	}

	testCases := []testCase{
		{[]string{"abc=cba"}, nil},
		{[]string{"a="}, ErrInvalidInput},
		{[]string{"a!="}, ErrInvalidInput},
		{[]string{"=abc"}, ErrInvalidInput},
		{[]string{"@#$%!=(*&"}, ErrInvalidInput},
		{[]string{"!@#$%=(*&"}, ErrInvalidInput},
		{[]string{"@#;$%=(*&"}, ErrInvalidInput},
		{[]string{"@#$%=(;*&"}, ErrInvalidInput},
		{[]string{"@#$%=(*&"}, nil},
		{[]string{"@#$%=(*&", "abc=!fd", "a===="}, nil},
		{[]string{"@#$%=(*&", "abc=!fd", "a===;="}, ErrInvalidInput},
		{[]string{"a=~a"}, ErrInvalidInput},
		{[]string{"a=a~"}, nil},
		{[]string{"aaa"}, ErrInvalidInput},
		{[]string{"aaa=b\xc3"}, ErrInvalidInput},
		{[]string{"a\xc3=bb\x28\xc5"}, ErrInvalidInput},
	}

	for _, tc := range testCases {
		err := ValidateTags(tc.tag)
		if tc.expecting != err {
			t.Fatalf("Expected %t, but testcase %s returned %v", tc.expecting, tc.tag, err)
		}
	}
}

func newMetricDefinition(name string, tags []string) *MetricDefinition {
	sort.Strings(tags)

	return &MetricDefinition{Name: name, Tags: tags}
}

func TestNameWithTags(t *testing.T) {
	type testCase struct {
		expectedName         string
		expectedNameWithTags string
		expectedTags         []string
		md                   MetricDefinition
	}

	testCases := []testCase{
		{
			"a.b.c",
			"a.b.c;tag1=value1",
			[]string{"tag1=value1"},
			*newMetricDefinition("a.b.c", []string{"tag1=value1", "name=ccc"}),
		}, {
			"a.b.c",
			"a.b.c;a=a;b=b;c=c",
			[]string{"a=a", "b=b", "c=c"},
			*newMetricDefinition("a.b.c", []string{"name=a.b.c", "c=c", "b=b", "a=a"}),
		}, {
			"a.b.c",
			"a.b.c",
			[]string{},
			*newMetricDefinition("a.b.c", []string{"name=a.b.c"}),
		}, {
			"a.b.c",
			"a.b.c",
			[]string{},
			*newMetricDefinition("a.b.c", []string{}),
		}, {
			"c",
			"c;a=a;b=b;c=c",
			[]string{"a=a", "b=b", "c=c"},
			*newMetricDefinition("c", []string{"c=c", "a=a", "b=b"}),
		},
	}

	for _, tc := range testCases {
		tc.md.SetId()
		if tc.expectedName != tc.md.Name {
			t.Fatalf("Expected name %s, but got %s", tc.expectedName, tc.md.Name)
		}

		if tc.expectedNameWithTags != tc.md.NameWithTags() {
			t.Fatalf("Expected name with tags %s, but got %s", tc.expectedNameWithTags, tc.md.NameWithTags())
		}

		if len(tc.expectedTags) != len(tc.md.Tags) {
			t.Fatalf("Expected tags %+v, but got %+v", tc.expectedTags, tc.md.Tags)
		}

		for i := range tc.expectedTags {
			if len(tc.expectedTags[i]) != len(tc.md.Tags[i]) {
				t.Fatalf("Expected tags %+v, but got %+v", tc.expectedTags, tc.md.Tags)
			}
		}

		getAddress := func(s string) uint {
			return uint((*reflect.StringHeader)(unsafe.Pointer(&s)).Data)
		}

		nameWithTagsAddr := getAddress(tc.md.NameWithTags())
		nameAddr := getAddress(tc.md.Name)
		if nameAddr != nameWithTagsAddr {
			t.Fatalf("Name slice does not appear to be slice of base string, %d != %d", nameAddr, nameWithTagsAddr)
		}

		for i := range tc.md.Tags {
			tagAddr := getAddress(tc.md.Tags[i])

			if tagAddr < nameWithTagsAddr || tagAddr >= nameWithTagsAddr+uint(len(tc.md.NameWithTags())) {
				t.Fatalf("Tag slice does not appear to be slice of base string, %d != %d", tagAddr, nameWithTagsAddr)
			}
		}
	}
}

func TestNameSanitizedAsTagValue(t *testing.T) {
	type testCase struct {
		originalName string
		expectedName string
	}
	cases := []testCase{
		{
			originalName: "my~.test.abc",
			expectedName: "my~.test.abc",
		}, {
			originalName: "~a.b.c",
			expectedName: "a.b.c",
		}, {
			originalName: "~~a~~.~~~b~~~.~~~c~~~",
			expectedName: "a~~.~~~b~~~.~~~c~~~",
		}, {
			originalName: "~a~",
			expectedName: "a~",
		}, {
			originalName: "~",
			expectedName: "",
		}, {
			originalName: "~~~",
			expectedName: "",
		},
	}
	for i := range cases {
		md := MetricDefinition{Name: cases[i].originalName}
		sanitized := md.NameSanitizedAsTagValue()
		if sanitized != cases[i].expectedName {
			t.Fatalf("TC %d: Expected sanitized version of %s to be %s, but it was %s", i, md.Name, cases[i].expectedName, sanitized)
		}
	}
}

func BenchmarkNameSanitizedAsTagValueWithValidValue(b *testing.B) {
	inputValue := "some.id.of.a.metric.1"

	for i := 0; i < b.N; i++ {
		SanitizeNameAsTagValue(inputValue)
	}
}
func BenchmarkNameSanitizedAsTagValueWithInvalidValue(b *testing.B) {
	inputValue := "~some.id.of.a.metric.1"

	for i := 0; i < b.N; i++ {
		SanitizeNameAsTagValue(inputValue)
	}
}
