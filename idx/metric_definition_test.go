package idx

import (
	"fmt"
	"reflect"
	"testing"
	"unsafe"

	goi "github.com/robert-milan/go-object-interning"
	. "github.com/smartystreets/goconvey/convey"
)

var metName = "metric.names.can.be.a.bit.longer.than.normal.sometimes"
var metNameRepeating = "metric.metric.metric.metric.metric.metric.metric.metric"
var testSz string

func genTags(num int) []string {
	szs := make([]string, num)
	for i := 0; i < num; i++ {
		szs[i] = fmt.Sprintf("key%d=val%d", i, i)
	}
	return szs
}

func genMetricDefinitionsWithSameName(num int) []*MetricDefinition {
	defs := make([]*MetricDefinition, num)
	for i := 0; i < num; i++ {
		defs[i] = &MetricDefinition{
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

func genMetricDefinitionsWithoutTags(num int) []*MetricDefinition {
	defs := make([]*MetricDefinition, num)
	for i := 0; i < num; i++ {
		defs[i] = &MetricDefinition{
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

func TestCreateDeleteMetricDefinition10(t *testing.T) {
	testCreateDeleteMetricDefinition(t, 10)
}

func TestCreateDeleteMetricDefinition1000(t *testing.T) {
	testCreateDeleteMetricDefinition(t, 1000)
}

func testCreateDeleteMetricDefinition(t *testing.T, num int) {
	IdxIntern = goi.NewObjectIntern(nil)
	defs := genMetricDefinitionsWithSameName(num)
	name := "anotheryetlonger.short.metric.name"

	originalNameAddress := defs[0].Name.Nodes()[0]

	Convey("When creating MetricDefinitions", t, func() {
		Convey(fmt.Sprintf("reference counts should be at %d", num), func() {
			for _, md := range defs {
				for _, ptr := range md.Name.Nodes() {
					cnt, err := IdxIntern.RefCnt(ptr)
					So(err, ShouldBeNil)
					So(cnt, ShouldEqual, num)
				}

			}
		})
		Convey(fmt.Sprintf("After deleting half of the metricdefinitions reference count should be %d", num/2), func() {
			for i := 0; i < num/2; i++ {
				InternReleaseMetricDefinition(*defs[i])
				defs[i] = nil
			}
			defs = defs[num/2:]
			for _, md := range defs {
				for _, ptr := range md.Name.Nodes() {
					cnt, err := IdxIntern.RefCnt(ptr)
					So(err, ShouldBeNil)
					So(cnt, ShouldEqual, num/2)
				}

			}
		})
		Convey("Name should still be the same when retrieved", func() {
			for _, md := range defs {
				So(md.Name.String(), ShouldEqual, name)
			}
		})
		Convey("After deleting all of the metricdefinitions the name should be removed from the object store", func() {
			nameAddr := defs[0].Name.Nodes()[0]
			for i := 0; i < len(defs); i++ {
				InternReleaseMetricDefinition(*defs[i])
				defs[i] = nil
			}
			cnt, err := IdxIntern.RefCnt(nameAddr)
			So(err, ShouldNotBeNil)
			So(cnt, ShouldEqual, 0)
		})
		Convey("After adding more metricdefinitions with the same name as before we should have a new object address for their names", func() {
			// create this to use the first memory offset of the new slab in a fresh slabPool in case
			// MMap decides to use the same memory chunk. The string is the same length as what should be in slot 0.
			IdxIntern.AddOrGetString([]byte("bopuifszfumpohfs"))
			defs := genMetricDefinitionsWithSameName(num)
			So(originalNameAddress, ShouldNotEqual, defs[0].Name.Nodes()[0])
		})
	})
}

func TestMetricNameAndTagAddresses(t *testing.T) {
	IdxIntern = goi.NewObjectIntern(nil)
	defs := make([]*MetricDefinition, 5)
	for i := 0; i < len(defs); i++ {
		defs[i] = &MetricDefinition{
			OrgId:      uint32(i),
			Interval:   10,
			LastUpdate: 0,
			Partition:  1,
		}
		defs[i].SetTags(genTags(5))
		defs[i].SetMType("rate")
		defs[i].SetMetricName("some.short.metric.name")
		defs[i].SetUnit("test")
		defs[i].SetId()
	}

	Convey("When creating MetricDefinitions with the same name and tags", t, func() {
		Convey("names should be using the same object addresses", func() {
			for _, md := range defs {
				for _, mdd := range defs {
					So(md.Name.Nodes(), ShouldResemble, mdd.Name.Nodes())
				}
			}
		})
		Convey("reference counts should be at 5", func() {
			for _, md := range defs {
				for _, ptr := range md.Name.Nodes() {
					cnt, err := IdxIntern.RefCnt(ptr)
					So(err, ShouldBeNil)
					So(cnt, ShouldEqual, 5)
				}

			}
		})
		Convey("tags should be using the same object addresses", func() {
			for idx, tag := range defs[0].Tags {
				keyData := (*reflect.StringHeader)(unsafe.Pointer(&tag.Key)).Data
				valData := (*reflect.StringHeader)(unsafe.Pointer(&tag.Value)).Data
				for i := 1; i < len(defs); i++ {
					So(keyData, ShouldEqual, (*reflect.StringHeader)(unsafe.Pointer(&defs[i].Tags[idx].Key)).Data)
					So(valData, ShouldEqual, (*reflect.StringHeader)(unsafe.Pointer(&defs[i].Tags[idx].Value)).Data)
				}
			}
		})
	})
}

func TestTagKeyValuesAndNameWithTags(t *testing.T) {
	IdxIntern = goi.NewObjectIntern(nil)
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
		testSz = defs[0].Name.String()
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
