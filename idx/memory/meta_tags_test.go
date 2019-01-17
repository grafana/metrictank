package memory

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestInsertSimpleMetaTagRecord(t *testing.T) {
	metaTags := []string{"metaTag1=abc", "anotherTag=theValue"}
	tagQueries := []string{"metricTag!=a", "match=~this"}

	metaTagRecords := NewMetaTagRecords()

	Convey("When adding a simple meta tag record", t, func() {
		updated, err := metaTagRecords.metaTagRecordUpsert(metaTags, tagQueries)
		So(err, ShouldBeNil)
		So(updated, ShouldBeNil)

		Convey("then it should exist in the meta tag records object", func() {
			So(len(metaTagRecords.records), ShouldEqual, 1)

			// we know there should only be one record, so we get it like this
			var record metaTagRecord
			for _, record = range metaTagRecords.records {
			}

			So(len(record.metaTags), ShouldEqual, 2)
			So(len(record.queries), ShouldEqual, 2)

			So(record.metaTags[0].key, ShouldEqual, "anotherTag")
			So(record.metaTags[0].value, ShouldEqual, "theValue")
			So(record.metaTags[1].key, ShouldEqual, "metaTag1")
			So(record.metaTags[1].value, ShouldEqual, "abc")
			So(record.queries[0].key, ShouldEqual, "match")
			So(record.queries[0].value, ShouldEqual, "this")
			So(record.queries[0].operator, ShouldEqual, MATCH)
			So(record.queries[1].key, ShouldEqual, "metricTag")
			So(record.queries[1].value, ShouldEqual, "a")
			So(record.queries[1].operator, ShouldEqual, NOT_EQUAL)
		})
	})
}

func TestUpdateExistingMetaTagRecord(t *testing.T) {
	// define the values for two metric records
	metaTags1 := []string{"metaTag1=value1"}
	tagQueries1 := []string{"tag1=a", "tag2=b"}
	metaTags2 := []string{"metaTag1=value1"}
	tagQueries2 := []string{"tag1=c", "tag2=d"}

	// define the values for an update which is going to replace
	// the first metric record because it has the same tag queries
	metaTagsUpdate := []string{"metaTag1=value2"}
	tagQueriesUpdate := []string{"tag1=a", "tag2=b"}

	metaTagRecords := NewMetaTagRecords()

	Convey("When adding two meta tag records", t, func() {
		updated, err := metaTagRecords.metaTagRecordUpsert(metaTags1, tagQueries1)
		So(err, ShouldBeNil)
		So(updated, ShouldBeNil)

		updated, err = metaTagRecords.metaTagRecordUpsert(metaTags2, tagQueries2)
		So(err, ShouldBeNil)
		So(updated, ShouldBeNil)

		Convey("then they should exist", func() {
			So(len(metaTagRecords.records), ShouldEqual, 2)

			// the order of the records may have changed due to sorting by hash
			var record1, record2 metaTagRecord
			for hash, record := range metaTagRecords.records {
				So(len(record.queries), ShouldEqual, 2)
				switch record.queries[0].value {
				case "a":
					record1 = metaTagRecords.records[hash]
				case "c":
					record2 = metaTagRecords.records[hash]
				}
			}

			// verify that we got valid records
			So(len(record1.queries), ShouldNotEqual, 0)
			So(len(record2.queries), ShouldNotEqual, 0)

			// verify that all the values are as expected
			So(record1.metaTags[0].key, ShouldEqual, "metaTag1")
			So(record1.metaTags[0].value, ShouldEqual, "value1")
			So(record1.queries[0].key, ShouldEqual, "tag1")
			So(record1.queries[0].value, ShouldEqual, "a")
			So(record1.queries[1].key, ShouldEqual, "tag2")
			So(record1.queries[1].value, ShouldEqual, "b")
			So(record2.metaTags[0].key, ShouldEqual, "metaTag1")
			So(record2.metaTags[0].value, ShouldEqual, "value1")
			So(record2.queries[0].key, ShouldEqual, "tag1")
			So(record2.queries[0].value, ShouldEqual, "c")
			So(record2.queries[1].key, ShouldEqual, "tag2")
			So(record2.queries[1].value, ShouldEqual, "d")

			Convey("then we update one of the records", func() {
				updated, err := metaTagRecords.metaTagRecordUpsert(metaTagsUpdate, tagQueriesUpdate)
				So(err, ShouldBeNil)
				So(updated, ShouldNotBeNil)

				Convey("then we should be able to see one old and one updated record", func() {
					So(len(metaTagRecords.records), ShouldEqual, 2)

					// the order of the records may have changed again due to sorting by hash
					for hash, record := range metaTagRecords.records {
						So(len(record.queries), ShouldEqual, 2)
						switch record.queries[0].value {
						case "a":
							record1 = metaTagRecords.records[hash]
						case "c":
							record2 = metaTagRecords.records[hash]
						}
					}

					// verify that we got valid records
					So(len(record1.queries), ShouldNotEqual, 0)
					So(len(record2.queries), ShouldNotEqual, 0)

					// verify that all the values are as expected
					So(record1.metaTags[0].key, ShouldEqual, "metaTag1")
					So(record1.metaTags[0].value, ShouldEqual, "value2")
					So(record1.queries[0].key, ShouldEqual, "tag1")
					So(record1.queries[0].value, ShouldEqual, "a")
					So(record1.queries[1].key, ShouldEqual, "tag2")
					So(record1.queries[1].value, ShouldEqual, "b")
					So(record2.metaTags[0].key, ShouldEqual, "metaTag1")
					So(record2.metaTags[0].value, ShouldEqual, "value1")
					So(record2.queries[0].key, ShouldEqual, "tag1")
					So(record2.queries[0].value, ShouldEqual, "c")
					So(record2.queries[1].key, ShouldEqual, "tag2")
					So(record2.queries[1].value, ShouldEqual, "d")
				})
			})
		})
	})
}
