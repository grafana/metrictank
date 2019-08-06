package interning

import (
	"fmt"
	"testing"

	goi "github.com/robert-milan/go-object-interning"
	. "github.com/smartystreets/goconvey/convey"
)

var metName = "metric.names.can.be.a.bit.longer.than.normal.sometimes"
var metNameRepeating = "metric.metric.metric.metric.metric.metric.metric.metric"
var testSz string

func genTags(num int) []string {
	tags := make([]string, num)
	for i := 0; i < num; i++ {
		tags[i] = fmt.Sprintf("key%d=val%d", i, i)
	}
	return tags
}

func genMetricDefinitionsWithSameName(num int) []*MetricDefinitionInterned {
	defs := make([]*MetricDefinitionInterned, num)
	for i := 0; i < num; i++ {
		defs[i] = &MetricDefinitionInterned{
			OrgId:      1,
			Interval:   10,
			LastUpdate: 0,
			Partition:  1,
		}
		defs[i].SetTags(genTags(5))
		defs[i].SetMType("rate")
		defs[i].SetMetricName("anotheryetlonger.short.metric.name")
		defs[i].SetUnit("test")
		defs[i].SetId()
	}
	return defs
}

func genMetricDefinitionsWithoutTags(num int) []*MetricDefinitionInterned {
	defs := make([]*MetricDefinitionInterned, num)
	for i := 0; i < num; i++ {
		defs[i] = &MetricDefinitionInterned{
			OrgId:      1,
			Interval:   10,
			LastUpdate: 0,
			Partition:  1,
		}
		defs[i].SetMType("rate")
		defs[i].SetMetricName(fmt.Sprintf("metric%d", i))
		defs[i].SetUnit("test")
		defs[i].SetId()
	}
	return defs
}

func TestTagKeyValuesAndNameWithTags(t *testing.T) {
	IdxIntern = goi.NewObjectIntern(goi.NewConfig())
	defs := genMetricDefinitionsWithSameName(1)
	tags := genTags(5)
	defs[0].SetTags(tags)

	Convey("After adding tags to a MetricDefinition", t, func() {
		Convey("the Strings() function of TagKeyValues should resemble the original []string", func() {
			So(tags, ShouldResemble, defs[0].Tags.Strings())
		})
		Convey("and NameWithTags should be predictable", func() {
			nwt := defs[0].NameWithTags()
			expected := "anotheryetlonger.short.metric.name;key0=val0;key1=val1;key2=val2;key3=val3;key4=val4"
			So(nwt, ShouldEqual, expected)
		})
	})
}

func BenchmarkSetTags10(b *testing.B) {
	benchmarkSetTags(b, 10)
}

func BenchmarkSetTags100(b *testing.B) {
	benchmarkSetTags(b, 100)
}

func BenchmarkSetTags1000(b *testing.B) {
	benchmarkSetTags(b, 1000)
}

func benchmarkSetTags(b *testing.B, num int) {
	tags := genTags(num)
	defs := genMetricDefinitionsWithoutTags(1)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		defs[0].SetTags(tags)
	}
}

func BenchmarkSetMetricName(b *testing.B) {
	defs := genMetricDefinitionsWithoutTags(1)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		defs[0].SetMetricName(metName)
	}
}

func BenchmarkSetMetricNameRepeatingWords(b *testing.B) {
	defs := genMetricDefinitionsWithoutTags(1)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		defs[0].SetMetricName(metNameRepeating)
	}
}

func BenchmarkGetMetricName(b *testing.B) {
	defs := genMetricDefinitionsWithoutTags(1)
	defs[0].SetMetricName(metName)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		testSz = defs[0].Name
	}
}

func BenchmarkGetNameWithTags(b *testing.B) {
	defs := genMetricDefinitionsWithSameName(1)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		testSz = defs[0].NameWithTags()
	}
}

func BenchmarkGetTags(b *testing.B) {
	defs := genMetricDefinitionsWithSameName(1)
	var tags []string

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tags = defs[0].Tags.Strings()
	}
	if len(tags) != 5 {
		panic("incorrect number of tags returned")
	}
}
