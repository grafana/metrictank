package idx

import (
	"fmt"
	"reflect"
	"runtime"
	"testing"
	"time"
	"unsafe"

	"github.com/raintank/schema"
	. "github.com/smartystreets/goconvey/convey"
)

func TestCreateMetricDefinitionFromMetricData(t *testing.T) {
	Convey("When creating a metric definition from metric data", t, func() {
		metricData := &schema.MetricData{
			Id:       "1.01234567890123456789012345678901",
			OrgId:    123,
			Name:     "myname.one.two.three",
			Interval: 60,
			Value:    1000,
			Time:     42,
			Mtype:    "gauge",
			Tags:     []string{"tag1=value1", "tag2=value2"},
		}
		metricDefinition, _ := MetricDefinitionFromMetricData(metricData)

		Convey("The name should be the same", func() {
			So(metricDefinition.Name.String(), ShouldEqual, metricData.Name)
		})

		Convey("The number of tags should be the same", func() {
			So(len(metricDefinition.Tags), ShouldEqual, len(metricData.Tags))
		})

		for i := range metricData.Tags {
			Convey(fmt.Sprintf("The tag keys&values should be the same in pair %d", i), func() {
				So(metricDefinition.Tags[i].String(), ShouldEqual, metricData.Tags[i])
			})
		}
	})
}

func TestNameNodesAreDeduplicatedInStringPool(t *testing.T) {
	Convey("When creating a metric definitions with repeating strings", t, func() {
		metricData1 := &schema.MetricData{
			Id:    "1.01234567890123456789012345678901",
			OrgId: 123,
			Name:  "aaa.bbb.aaa.one",
		}
		metricDefinition1, _ := MetricDefinitionFromMetricData(metricData1)

		metricData2 := &schema.MetricData{
			Id:    "1.01234567890123456789012345678902",
			OrgId: 123,
			Name:  "aaa.bbb.aaa.two",
		}
		metricDefinition2, _ := MetricDefinitionFromMetricData(metricData2)

		Convey("The string aaa should be deduplicated correctly", func() {
			So(
				(*reflect.StringHeader)(unsafe.Pointer(&metricDefinition1.Name.nodes[0])).Data,
				ShouldEqual,
				(*reflect.StringHeader)(unsafe.Pointer(&metricDefinition2.Name.nodes[0])).Data,
			)
		})

		Convey("The string aaa should be deduplicated correctly even if it is at a different position in the name", func() {
			So(
				(*reflect.StringHeader)(unsafe.Pointer(&metricDefinition1.Name.nodes[0])).Data,
				ShouldEqual,
				(*reflect.StringHeader)(unsafe.Pointer(&metricDefinition2.Name.nodes[2])).Data,
			)
		})

		Convey("The string bbb should be deduplicated correctly as well", func() {
			So(
				(*reflect.StringHeader)(unsafe.Pointer(&metricDefinition1.Name.nodes[1])).Data,
				ShouldEqual,
				(*reflect.StringHeader)(unsafe.Pointer(&metricDefinition2.Name.nodes[1])).Data,
			)
		})

		Convey("The string aaa should not be deduplicated with bbb", func() {
			So(
				(*reflect.StringHeader)(unsafe.Pointer(&metricDefinition1.Name.nodes[0])).Data,
				ShouldNotEqual,
				(*reflect.StringHeader)(unsafe.Pointer(&metricDefinition2.Name.nodes[1])).Data,
			)
		})
	})
}

func TestTagStringsAreDeduplicatedInStringPool(t *testing.T) {
	Convey("When creating a metric definitions with repeating tag keys, but differing values", t, func() {
		metricData1 := &schema.MetricData{
			Id:    "1.01234567890123456789012345678901",
			OrgId: 123,
			Name:  "a",
			Tags:  []string{"key1=value1", "key2=value2"},
		}
		metricDefinition1, _ := MetricDefinitionFromMetricData(metricData1)

		metricData2 := &schema.MetricData{
			Id:    "1.01234567890123456789012345678902",
			OrgId: 123,
			Name:  "aaa.bbb.aaa.two",
			Tags:  []string{"key1=value2", "key2=value1"},
		}
		metricDefinition2, _ := MetricDefinitionFromMetricData(metricData2)

		Convey("The tag-keys with value \"key1\" should be deduplicated correctly", func() {
			So(
				(*reflect.StringHeader)(unsafe.Pointer(&metricDefinition1.Tags[0].Key)).Data,
				ShouldEqual,
				(*reflect.StringHeader)(unsafe.Pointer(&metricDefinition2.Tags[0].Key)).Data,
			)
		})
		Convey("The tag-values with different values should not be deduplicated", func() {
			So(
				(*reflect.StringHeader)(unsafe.Pointer(&metricDefinition1.Tags[0].Value)).Data,
				ShouldNotEqual,
				(*reflect.StringHeader)(unsafe.Pointer(&metricDefinition2.Tags[0].Value)).Data,
			)
		})
	})
}

func TestUnreferencedStringsGetCleanedUp(t *testing.T) {
	// first run the GC because the pool might still have elements of previous tests
	runtime.GC()

	Convey("When creating a metric definition", t, func() {
		metricData := &schema.MetricData{Id: "1.01234567890123456789012345678901", OrgId: 1, Name: "a.b.c"}

		Convey("The count of name-nodes should be reflected in the size of the string pool", func() {
			metricDefinition, _ := MetricDefinitionFromMetricData(metricData)
			So(
				len(stringPool.terms),
				ShouldEqual,
				3,
			)
			// need to do something with the metricDefinition variable to make go
			// not complain about it being unused
			metricDefinition.Name.String()
		})

		Convey("After GC the string pool should be cleaned up", func() {
			// this should clean up everything in the string pool,
			// because metricDefinition has gone out of context
			runtime.GC()

			// short sleep for GC
			time.Sleep(1)

			So(
				len(stringPool.terms),
				ShouldEqual,
				0,
			)
		})
	})
}
