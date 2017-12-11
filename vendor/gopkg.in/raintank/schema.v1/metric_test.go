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
		Metric:   "my.test.metric.name",
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

func TestTagValidation(t *testing.T) {
	type testCase struct {
		tag       []string
		expecting bool
	}

	testCases := []testCase{
		{[]string{"abc=cba"}, true},
		{[]string{"a="}, false},
		{[]string{"a!="}, false},
		{[]string{"=abc"}, false},
		{[]string{"@#$%!=(*&"}, false},
		{[]string{"!@#$%=(*&"}, false},
		{[]string{"@#;$%=(*&"}, false},
		{[]string{"@#$%=(;*&"}, false},
		{[]string{"@#$%=(*&"}, true},
		{[]string{"@#$%=(*&", "abc=!fd", "a===="}, true},
		{[]string{"@#$%=(*&", "abc=!fd", "a===;="}, false},
	}

	for _, tc := range testCases {
		if tc.expecting != validateTags(tc.tag) {
			t.Fatalf("Expected %t, but testcase %s returned %t", tc.expecting, tc.tag, !tc.expecting)
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
