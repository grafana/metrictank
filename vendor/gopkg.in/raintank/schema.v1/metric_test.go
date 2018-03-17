package schema

import (
	"math"
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
		if tc.expecting != ValidateTags(tc.tag) {
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

func TestMetricPointId1MarshalDirect(t *testing.T) {
	tests := []MetricPointId1{
		{
			Id:    [16]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 255},
			Time:  0,
			Value: 0,
		},
		{
			Id:    [16]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			Time:  1234567890,
			Value: 123.456789,
		},
		{
			Id:    [16]byte{255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255},
			Time:  math.MaxUint32,
			Value: math.MaxFloat64,
		},
	}
	for i, in := range tests {
		// marshal with nil input
		data1, err := in.MarshalDirect(nil)
		if err != nil {
			t.Fatalf("case %d got err %s", i, err.Error())
		}
		if len(data1) != 28 {
			t.Fatalf("case %d did not result in 28B data packet", i)
		}

		// marshal with sufficient input
		buf2 := make([]byte, 0, 28)
		data2, err := in.MarshalDirect(buf2)
		if err != nil {
			t.Fatalf("case %d got err %s", i, err.Error())
		}
		if !reflect.DeepEqual(data1, data2[:28]) {
			t.Fatalf("case %d marshaling mismatch.", i)
		}

		// marshal with very big input
		buf3 := make([]byte, 0, 512)
		data3, err := in.MarshalDirect(buf3)
		if err != nil {
			t.Fatalf("case %d got err %s", i, err.Error())
		}
		if !reflect.DeepEqual(data1, data3[:28]) {
			t.Fatalf("case %d marshaling mismatch", i)
		}

		// MarshalDirect28 with sufficient input
		buf4 := make([]byte, 0, 28)
		data4, err := in.MarshalDirect28(buf4)
		if err != nil {
			t.Fatalf("case %d got err %s", i, err.Error())
		}
		if !reflect.DeepEqual(data1, data4[:28]) {
			t.Fatalf("case %d marshaling mismatch.", i)
		}

		// MarshalDirect28 with very big input
		buf5 := make([]byte, 0, 512)
		data5, err := in.MarshalDirect28(buf5)
		if err != nil {
			t.Fatalf("case %d got err %s", i, err.Error())
		}
		if !reflect.DeepEqual(data1, data5[:28]) {
			t.Fatalf("case %d marshaling mismatch", i)
		}

		// pre-existing input that should be left alone
		buf6 := []byte{'f', 'o', 'o'}
		data6, err := in.MarshalDirect(buf6)
		if err != nil {
			t.Fatalf("case %d got err %s", i, err.Error())
		}
		if !reflect.DeepEqual(data1, data6[3:]) {
			t.Fatalf("case %d marshaling mismatch", i)
		}
		if string(data6[0:3]) != "foo" {
			t.Fatalf("case %d pre-existing data was modified to %q", i, string(data6[0:3]))
		}

		// unmarshal

		out := MetricPointId1{}
		leftover, err := out.UnmarshalDirect(data1)
		if err != nil {
			t.Fatalf("case %d got err %s", i, err.Error())
		}
		if len(leftover) != 0 {
			t.Fatalf("case %d got leftover data: %v", i, leftover)
		}
		if in.Id != out.Id || in.Time != out.Time || in.Value != out.Value {
			t.Fatalf("case %d data mismatch:\nexp: %v\ngot: %v", i, in, out)
		}
	}
}

func TestMetricPointId1MarshalDirectMultiple(t *testing.T) {
	tests := []MetricPointId1{
		{
			Id:    [16]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 255},
			Time:  0,
			Value: 0,
		},
		{
			Id:    [16]byte{255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255},
			Time:  math.MaxUint32,
			Value: math.MaxFloat64,
		},
		{
			Id:    [16]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 255},
			Time:  0,
			Value: 0,
		},
		{
			Id:    [16]byte{255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255},
			Time:  math.MaxUint32,
			Value: math.MaxFloat64,
		},
	}
	var buf []byte
	for i, in := range tests {
		// marshal with nil input
		var err error
		buf, err = in.MarshalDirect(buf)
		if err != nil {
			t.Fatalf("case %d got err %s", i, err.Error())
		}
		expLen := (i + 1) * 28
		if len(buf) != expLen {
			t.Fatalf("iteration %d . expected length %d, got %d", i, expLen, len(buf))
		}
	}
	type byteCheck struct {
		pos     int
		val     uint8
		comment string
	}
	checks := []byteCheck{
		{0, 0, "byte 0 of id of first"},
		{12, 12, "byte 12 of id of first"},
		{27, 0, "last byte of value of first"},
		{28*2 + 0, 0, "byte 0 of id of third"},
		{28*2 + 12, 12, "byte 12 of id of third"},
		{28*2 + 27, 0, "last byte of value of third"},

		{28 + 0, 255, "byte 0 of id of second"},
		{28 + 12, 255, "byte 12 of id of second"},
		{28 + 27, 255, "last byte of value of second"},
		{28*3 + 0, 255, "byte 0 of id of fourth"},
		{28*3 + 12, 255, "byte 12 of id of fourth"},
		{28*3 + 27, 255, "last byte of value of fourth"},
	}
	for _, check := range checks {
		if buf[check.pos] != check.val {
			t.Fatalf("%s: expected val %d, got %d", check.comment, check.val, buf[check.pos])
		}
	}
}

func TestMetricPointId2MarshalDirect(t *testing.T) {
	tests := []MetricPointId2{
		{
			MetricPointId1{
				Id:    [16]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 255},
				Time:  0,
				Value: 0,
			},
			0,
		},
		{
			MetricPointId1{
				Id:    [16]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
				Time:  1234567890,
				Value: 123.456789,
			},
			2812345,
		},
		{
			MetricPointId1{
				Id:    [16]byte{255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255},
				Time:  math.MaxUint32,
				Value: math.MaxFloat64,
			},
			math.MaxUint32,
		},
	}
	for i, in := range tests {
		// marshal with nil input
		data1, err := in.MarshalDirect(nil)
		if err != nil {
			t.Fatalf("case %d got err %s", i, err.Error())
		}
		if len(data1) != 32 {
			t.Fatalf("case %d did not result in 32B data packet", i)
		}

		// marshal with sufficient input
		buf2 := make([]byte, 0, 32)
		data2, err := in.MarshalDirect(buf2)
		if err != nil {
			t.Fatalf("case %d got err %s", i, err.Error())
		}
		if !reflect.DeepEqual(data1, data2[:32]) {
			t.Fatalf("case %d marshaling mismatch.", i)
		}

		// marshal with very big input
		buf3 := make([]byte, 0, 512)
		data3, err := in.MarshalDirect(buf3)
		if err != nil {
			t.Fatalf("case %d got err %s", i, err.Error())
		}
		if !reflect.DeepEqual(data1, data3[:32]) {
			t.Fatalf("case %d marshaling mismatch", i)
		}

		// MarshalDirect32 with sufficient input
		buf4 := make([]byte, 0, 32)
		data4, err := in.MarshalDirect32(buf4)
		if err != nil {
			t.Fatalf("case %d got err %s", i, err.Error())
		}
		if !reflect.DeepEqual(data1, data4[:32]) {
			t.Fatalf("case %d marshaling mismatch.", i)
		}

		// MarshalDirect32 with very big input
		buf5 := make([]byte, 0, 512)
		data5, err := in.MarshalDirect32(buf5)
		if err != nil {
			t.Fatalf("case %d got err %s", i, err.Error())
		}
		if !reflect.DeepEqual(data1, data5[:32]) {
			t.Fatalf("case %d marshaling mismatch", i)
		}

		// unmarshal

		out := MetricPointId2{}
		leftover, err := out.UnmarshalDirect(data1)
		if err != nil {
			t.Fatalf("case %d got err %s", i, err.Error())
		}
		if len(leftover) != 0 {
			t.Fatalf("case %d got leftover data: %v", i, leftover)
		}
		if in.Id != out.Id || in.Time != out.Time || in.Value != out.Value {
			t.Fatalf("case %d data mismatch:\nexp: %v\ngot: %v", i, in, out)
		}
	}
}
