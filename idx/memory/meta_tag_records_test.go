package memory

import (
	"hash"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestInsertSimpleMetaTagRecord(t *testing.T) {
	metaTags := []string{"metaTag1=abc", "anotherTag=theValue"}
	tagQueries := []string{"metricTag!=a", "match=~this"}

	metaTagRecords := make(metaTagRecords)

	Convey("When adding a simple meta tag record", t, func() {
		_, record, oldHash, oldRecord, err := metaTagRecords.upsert(metaTags, tagQueries)
		So(err, ShouldBeNil)
		So(record, ShouldNotBeNil)
		So(oldHash, ShouldBeZeroValue)
		So(oldRecord, ShouldBeNil)

		Convey("then it should exist in the meta tag records object", func() {
			So(len(metaTagRecords), ShouldEqual, 1)

			// we know there should only be one record, so we get it like this
			var record metaTagRecord
			for _, record = range metaTagRecords {
			}

			So(len(record.metaTags), ShouldEqual, 2)
			So(len(record.queries), ShouldEqual, 2)

			So(record.metaTags[1].key, ShouldEqual, "anotherTag")
			So(record.metaTags[1].value, ShouldEqual, "theValue")
			So(record.metaTags[0].key, ShouldEqual, "metaTag1")
			So(record.metaTags[0].value, ShouldEqual, "abc")
			So(record.queries[0].getKey(), ShouldEqual, "match")
			So(record.queries[0].getValue(), ShouldEqual, "^(?:this)")
			So(record.queries[0].getOperator(), ShouldEqual, opMatch)
			So(record.queries[1].getKey(), ShouldEqual, "metricTag")
			So(record.queries[1].getValue(), ShouldEqual, "a")
			So(record.queries[1].getOperator(), ShouldEqual, opNotEqual)
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

	metaTagRecords := make(metaTagRecords)

	Convey("When adding two meta tag records", t, func() {
		_, record, oldHash, oldRecord, err := metaTagRecords.upsert(metaTags1, tagQueries1)
		So(err, ShouldBeNil)
		So(record, ShouldNotBeNil)
		So(oldHash, ShouldBeZeroValue)
		So(oldRecord, ShouldBeNil)

		_, record, oldHash, oldRecord, err = metaTagRecords.upsert(metaTags2, tagQueries2)
		So(err, ShouldBeNil)
		So(record, ShouldNotBeNil)
		So(oldHash, ShouldBeZeroValue)
		So(oldRecord, ShouldBeNil)

		Convey("then they should exist", func() {
			So(len(metaTagRecords), ShouldEqual, 2)

			// the order of the records may have changed due to sorting by hash
			var record1, record2 metaTagRecord
			for hash, record := range metaTagRecords {
				So(len(record.queries), ShouldEqual, 2)
				switch record.queries[0].getValue() {
				case "a":
					record1 = metaTagRecords[hash]
				case "c":
					record2 = metaTagRecords[hash]
				}
			}

			// verify that we got valid records
			So(len(record1.queries), ShouldNotEqual, 0)
			So(len(record2.queries), ShouldNotEqual, 0)

			// verify that all the values are as expected
			So(record1.metaTags[0].key, ShouldEqual, "metaTag1")
			So(record1.metaTags[0].value, ShouldEqual, "value1")
			So(record1.queries[0].getKey(), ShouldEqual, "tag1")
			So(record1.queries[0].getValue(), ShouldEqual, "a")
			So(record1.queries[1].getKey(), ShouldEqual, "tag2")
			So(record1.queries[1].getValue(), ShouldEqual, "b")
			So(record2.metaTags[0].key, ShouldEqual, "metaTag1")
			So(record2.metaTags[0].value, ShouldEqual, "value1")
			So(record2.queries[0].getKey(), ShouldEqual, "tag1")
			So(record2.queries[0].getValue(), ShouldEqual, "c")
			So(record2.queries[1].getKey(), ShouldEqual, "tag2")
			So(record2.queries[1].getValue(), ShouldEqual, "d")

			Convey("then we update one of the records", func() {
				hash, record, oldHash, oldRecord, err := metaTagRecords.upsert(metaTagsUpdate, tagQueriesUpdate)
				So(err, ShouldBeNil)
				So(record, ShouldNotBeNil)
				So(oldHash, ShouldEqual, hash)
				So(oldRecord, ShouldNotBeNil)

				Convey("then we should be able to see one old and one updated record", func() {
					So(len(metaTagRecords), ShouldEqual, 2)

					// the order of the records may have changed again due to sorting by hash
					for hash, record := range metaTagRecords {
						So(len(record.queries), ShouldEqual, 2)
						switch record.queries[0].getValue() {
						case "a":
							record1 = metaTagRecords[hash]
						case "c":
							record2 = metaTagRecords[hash]
						}
					}

					// verify that we got valid records
					So(len(record1.queries), ShouldNotEqual, 0)
					So(len(record2.queries), ShouldNotEqual, 0)

					// verify that all the values are as expected
					So(record1.metaTags[0].key, ShouldEqual, "metaTag1")
					So(record1.metaTags[0].value, ShouldEqual, "value2")
					So(record1.queries[0].getKey(), ShouldEqual, "tag1")
					So(record1.queries[0].getValue(), ShouldEqual, "a")
					So(record1.queries[1].getKey(), ShouldEqual, "tag2")
					So(record1.queries[1].getValue(), ShouldEqual, "b")
					So(record2.metaTags[0].key, ShouldEqual, "metaTag1")
					So(record2.metaTags[0].value, ShouldEqual, "value1")
					So(record2.queries[0].getKey(), ShouldEqual, "tag1")
					So(record2.queries[0].getValue(), ShouldEqual, "c")
					So(record2.queries[1].getKey(), ShouldEqual, "tag2")
					So(record2.queries[1].getValue(), ShouldEqual, "d")
				})
			})
		})
	})
}

type mockHash struct {
	returnValues []uint32
	position     int
}

func (m *mockHash) Sum32() uint32 {
	value := m.returnValues[m.position%len(m.returnValues)]
	m.position = (m.position + 1) % len(m.returnValues)
	return value
}

func (m *mockHash) Write(p []byte) (n int, err error) {
	return
}

func (m *mockHash) Sum(b []byte) (res []byte) {
	return
}

func (m *mockHash) Reset() {}

func (m *mockHash) Size() (n int) {
	return
}
func (m *mockHash) BlockSize() (n int) {
	return
}

func getMockHash() hash.Hash32 {
	return &mockHash{
		returnValues: []uint32{1}, // keep returning 1
	}
}

func TestHashCollisionsOnInsert(t *testing.T) {
	originalHash := queryHash
	defer func() { queryHash = originalHash }()

	queryHash = getMockHash

	metaTags1 := []string{"metaTag1=value1"}
	tagQueries1 := []string{"metricTag1=value1"}
	metaTags2 := []string{"metaTag2=value2"}
	tagQueries2 := []string{"metricTag2=value2"}
	metaTags3 := []string{"metaTag3=value3"}
	tagQueries3 := []string{"metricTag3=value3"}
	metaTags4 := []string{"metaTag4=value4"}
	tagQueries4 := []string{"metricTag4=value4"}
	metaTagsUpdate := []string{"metaTag3=value4"}
	tagQueriesUpdate := []string{"metricTag3=value3"}

	Convey("When adding 3 meta records with the same hash", t, func() {
		metaTagRecords := make(metaTagRecords)

		hash, record, oldHash, oldRecord, err := metaTagRecords.upsert(metaTags1, tagQueries1)
		So(hash, ShouldEqual, 1)
		So(record.metaTags[0].key, ShouldEqual, "metaTag1")
		So(record.metaTags[0].value, ShouldEqual, "value1")
		So(record.queries[0].getKey(), ShouldEqual, "metricTag1")
		So(record.queries[0].getValue(), ShouldEqual, "value1")
		So(oldHash, ShouldBeZeroValue)
		So(oldRecord, ShouldBeNil)
		So(err, ShouldBeNil)

		hash, record, oldHash, oldRecord, err = metaTagRecords.upsert(metaTags2, tagQueries2)
		So(hash, ShouldEqual, 2)
		So(record.metaTags[0].key, ShouldEqual, "metaTag2")
		So(record.metaTags[0].value, ShouldEqual, "value2")
		So(record.queries[0].getKey(), ShouldEqual, "metricTag2")
		So(record.queries[0].getValue(), ShouldEqual, "value2")
		So(oldHash, ShouldBeZeroValue)
		So(oldRecord, ShouldBeNil)
		So(err, ShouldBeNil)

		hash, record, oldHash, oldRecord, err = metaTagRecords.upsert(metaTags3, tagQueries3)
		So(hash, ShouldEqual, 3)
		So(record.metaTags[0].key, ShouldEqual, "metaTag3")
		So(record.metaTags[0].value, ShouldEqual, "value3")
		So(record.queries[0].getKey(), ShouldEqual, "metricTag3")
		So(record.queries[0].getValue(), ShouldEqual, "value3")
		So(oldHash, ShouldBeZeroValue)
		So(oldRecord, ShouldBeNil)
		So(err, ShouldBeNil)

		So(len(metaTagRecords), ShouldEqual, 3)

		Convey("When adding a 4th record with the same hash", func() {
			hash, record, oldHash, oldRecord, err := metaTagRecords.upsert(metaTags4, tagQueries4)
			So(err, ShouldNotBeNil)
			So(hash, ShouldBeZeroValue)
			So(record, ShouldBeNil)
			So(oldHash, ShouldBeZeroValue)
			So(oldRecord, ShouldBeNil)
		})

		Convey("When updating the third record", func() {
			hash, record, oldHash, oldRecord, err := metaTagRecords.upsert(metaTagsUpdate, tagQueriesUpdate)
			So(err, ShouldBeNil)
			So(hash, ShouldEqual, 3)
			So(record.metaTags[0].key, ShouldEqual, "metaTag3")
			So(record.metaTags[0].value, ShouldEqual, "value4")
			So(record.queries[0].getKey(), ShouldEqual, "metricTag3")
			So(record.queries[0].getValue(), ShouldEqual, "value3")
			So(oldHash, ShouldEqual, 3)
			So(oldRecord.metaTags[0].key, ShouldEqual, "metaTag3")
			So(oldRecord.metaTags[0].value, ShouldEqual, "value3")
			So(oldRecord.queries[0].getKey(), ShouldEqual, "metricTag3")
			So(oldRecord.queries[0].getValue(), ShouldEqual, "value3")
		})
	})
}

func TestDeletingMetaRecord(t *testing.T) {
	metaTags1 := []string{"metaTag1=value1"}
	tagQueries1 := []string{"metricTag1=value1"}
	metaTags2 := []string{"metaTag2=value2"}
	tagQueries2 := []string{"metricTag2=value2"}
	metaTagsDelete := []string{}
	tagQueriesDelete := []string{"metricTag2=value2"}

	Convey("When adding 2 meta records", t, func() {
		metaTagRecords := make(metaTagRecords)

		hash, record, oldHash, oldRecord, err := metaTagRecords.upsert(metaTags1, tagQueries1)
		So(record.metaTags[0].key, ShouldEqual, "metaTag1")
		So(record.metaTags[0].value, ShouldEqual, "value1")
		So(record.queries[0].getKey(), ShouldEqual, "metricTag1")
		So(record.queries[0].getValue(), ShouldEqual, "value1")
		So(oldHash, ShouldBeZeroValue)
		So(oldRecord, ShouldBeNil)
		So(err, ShouldBeNil)
		So(len(metaTagRecords), ShouldEqual, 1)
		_, ok := metaTagRecords[hash]
		So(ok, ShouldBeTrue)

		hash, record, oldHash, oldRecord, err = metaTagRecords.upsert(metaTags2, tagQueries2)
		So(record.metaTags[0].key, ShouldEqual, "metaTag2")
		So(record.metaTags[0].value, ShouldEqual, "value2")
		So(record.queries[0].getKey(), ShouldEqual, "metricTag2")
		So(record.queries[0].getValue(), ShouldEqual, "value2")
		So(oldHash, ShouldBeZeroValue)
		So(oldRecord, ShouldBeNil)
		So(err, ShouldBeNil)
		So(len(metaTagRecords), ShouldEqual, 2)
		_, ok = metaTagRecords[hash]
		So(ok, ShouldBeTrue)

		hashOfRecord2 := hash

		Convey("then we delete one record again", func() {
			hash, record, oldHash, oldRecord, err = metaTagRecords.upsert(metaTagsDelete, tagQueriesDelete)
			So(err, ShouldBeNil)
			So(len(record.metaTags), ShouldEqual, 0)
			So(record.queries[0].getKey(), ShouldEqual, "metricTag2")
			So(record.queries[0].getValue(), ShouldEqual, "value2")
			So(oldHash, ShouldEqual, hashOfRecord2)
			So(oldRecord.metaTags[0].key, ShouldEqual, "metaTag2")
			So(oldRecord.metaTags[0].value, ShouldEqual, "value2")
			So(oldRecord.queries[0].getKey(), ShouldEqual, "metricTag2")
			So(oldRecord.queries[0].getValue(), ShouldEqual, "value2")
			So(len(metaTagRecords), ShouldEqual, 1)
			_, ok = metaTagRecords[hash]
			So(ok, ShouldBeFalse)
		})
	})
}

func TestDeletingMetaRecordThatIncludesRegex(t *testing.T) {
	metaTag := []string{"metaTag1=value1"}
	tagQuery := []string{"metricTag1=~abc[0-9]"}
	metaTagDelete := []string{}
	tagQueryDeleteOriginalValue := []string{"metricTag1=~abc[0-9]"}
	tagQueryDeleteAnchoredValue := []string{"metricTag1=~^(?:abc[0-9])"}

	Convey("When adding a meta record that includes a regex query", t, func() {
		metaTagRecords := make(metaTagRecords)

		hash, record, oldHash, oldRecord, err := metaTagRecords.upsert(metaTag, tagQuery)
		So(record.metaTags[0].key, ShouldEqual, "metaTag1")
		So(record.metaTags[0].value, ShouldEqual, "value1")
		So(record.queries[0].getKey(), ShouldEqual, "metricTag1")
		So(record.queries[0].getValue(), ShouldEqual, "^(?:abc[0-9])")
		So(record.queries[0].getOperator(), ShouldEqual, opMatch)
		So(oldHash, ShouldBeZeroValue)
		So(oldRecord, ShouldBeNil)
		So(err, ShouldBeNil)
		So(len(metaTagRecords), ShouldEqual, 1)
		_, ok := metaTagRecords[hash]
		So(ok, ShouldBeTrue)

		Convey("then we delete the record again by using the query with the original value", func() {
			hash, record, oldHash, oldRecord, err = metaTagRecords.upsert(metaTagDelete, tagQueryDeleteOriginalValue)
			So(err, ShouldBeNil)
			So(len(record.metaTags), ShouldEqual, 0)
			So(record.queries[0].getKey(), ShouldEqual, "metricTag1")
			So(record.queries[0].getValue(), ShouldEqual, "^(?:abc[0-9])")
			So(record.queries[0].getOperator(), ShouldEqual, opMatch)
			So(oldHash, ShouldEqual, hash)
			So(len(metaTagRecords), ShouldEqual, 0)
			_, ok = metaTagRecords[hash]
			So(ok, ShouldBeFalse)
		})

		Convey("then we delete the record again by using the query with the anchored value", func() {
			hash, record, oldHash, oldRecord, err = metaTagRecords.upsert(metaTagDelete, tagQueryDeleteAnchoredValue)
			So(err, ShouldBeNil)
			So(len(record.metaTags), ShouldEqual, 0)
			So(record.queries[0].getKey(), ShouldEqual, "metricTag1")
			So(record.queries[0].getValue(), ShouldEqual, "^(?:abc[0-9])")
			So(record.queries[0].getOperator(), ShouldEqual, opMatch)
			So(oldHash, ShouldEqual, hash)
			So(len(metaTagRecords), ShouldEqual, 0)
			_, ok = metaTagRecords[hash]
			So(ok, ShouldBeFalse)
		})
	})
}
