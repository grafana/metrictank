package memory

import (
	"hash"
	"reflect"
	"testing"

	"github.com/grafana/metrictank/expr/tagquery"
)

func TestInsertSimpleMetaTagRecord(t *testing.T) {
	metaTagRecords := make(metaTagRecords)
	recordToInsert, err := tagquery.ParseMetaTagRecord([]string{"metaTag1=abc", "anotherTag=theValue"}, []string{"metricTag!=a", "match=~this"})
	if err != nil {
		t.Fatalf("Unexpected error when parsing meta tag record: %q", err)
	}

	_, record, oldId, oldRecord, err := metaTagRecords.upsert(recordToInsert)
	if err != nil {
		t.Fatalf("Unexpected error on meta tag record upsert: %q", err)
	}
	if record == nil {
		t.Fatalf("Record was expected to not be nil, but it was")
	}
	if oldId != 0 {
		t.Fatalf("Old id was expected to be 0, but it was %d", oldId)
	}
	if oldRecord != nil {
		t.Fatalf("OldRecord was expected to be nil, but it was %+v", oldRecord)
	}
	if len(metaTagRecords) != 1 {
		t.Fatalf("metaTagRecords was expected to have 1 entry, but it had %d", len(metaTagRecords))
	}

	_, ok := metaTagRecords[metaTagRecords.hashMetaTagRecord(*record)]
	if !ok {
		t.Fatalf("We expected the record to be found at the index of its hash, but it wasn't")
	}

	if len(record.MetaTags) != 2 {
		t.Fatalf("The newly created record was expected to have 2 tags, but it had %d", len(record.MetaTags))
	}
	if len(record.Queries) != 2 {
		t.Fatalf("The newly created record was expected to have 2 queries, but it had %d", len(record.Queries))
	}

	var seenMetaTag1, seenMetaTag2 bool
	for _, metaTag := range record.MetaTags {
		if reflect.DeepEqual(metaTag, tagquery.Tag{Key: "metaTag1", Value: "abc"}) {
			seenMetaTag1 = true
		}
		if reflect.DeepEqual(metaTag, tagquery.Tag{Key: "anotherTag", Value: "theValue"}) {
			seenMetaTag2 = true
		}
	}

	if !seenMetaTag1 || !seenMetaTag2 {
		t.Fatalf("We expected both meta tags to be present in the record, but not both were: %t / %t", seenMetaTag1, seenMetaTag2)
	}

	var seenQuery1, seenQuery2 bool
	for _, query := range record.Queries {
		// ignore the compiled regex structs, as they can't reliably be compared
		query.Regex = nil

		if reflect.DeepEqual(query, tagquery.Expression{Tag: tagquery.Tag{Key: "metricTag", Value: "a"}, Operator: tagquery.NOT_EQUAL, RequiresNonEmptyValue: false, UsesRegex: false}) {
			seenQuery1 = true
		}
		if reflect.DeepEqual(query, tagquery.Expression{Tag: tagquery.Tag{Key: "match", Value: "^(?:this)"}, Operator: tagquery.MATCH, RequiresNonEmptyValue: true, UsesRegex: true}) {
			seenQuery2 = true
		}
	}

	if !seenQuery1 || !seenQuery2 {
		t.Fatalf("We expected both queries to be present in the record, but not both were: %t / %t", seenQuery1, seenQuery2)
	}
}

func TestUpdateExistingMetaTagRecord(t *testing.T) {
	// define the values for two metric records with the same meta tags, but different queries
	recordToInsert1, err := tagquery.ParseMetaTagRecord([]string{"metaTag1=value1"}, []string{"tag1=~a", "tag2=~b"})
	if err != nil {
		t.Fatalf("Unexpected error when parsing meta tag record: %q", err)
	}
	recordToInsert2, err := tagquery.ParseMetaTagRecord([]string{"metaTag1=value1"}, []string{"tag1=~c", "tag2=~d"})
	if err != nil {
		t.Fatalf("Unexpected error when parsing meta tag record: %q", err)
	}

	// define the values for an update which is going to replace
	// the first metric record because it has the same tag queries
	recordToUpdate, err := tagquery.ParseMetaTagRecord([]string{"metaTag1=value2"}, []string{"tag1=~a", "tag2=~b"})
	if err != nil {
		t.Fatalf("Unexpected error when parsing meta tag record: %q", err)
	}

	metaTagRecords := make(metaTagRecords)
	metaTagRecords.upsert(recordToInsert1)
	metaTagRecords.upsert(recordToInsert2)

	if len(metaTagRecords) != 2 {
		t.Fatalf("Expected 2 meta tag records, but there were %d", len(metaTagRecords))
	}

	// the order of the records may have changed due to sorting by id
	var record1, record2 tagquery.MetaTagRecord
	var found1, found2 bool
	var recordIdToUpdate recordId
	for id, record := range metaTagRecords {
		switch record.Queries[0].Value {
		case "^(?:a)":
			record1 = metaTagRecords[id]
			found1 = true
			recordIdToUpdate = id
		case "^(?:c)":
			record2 = metaTagRecords[id]
			found2 = true
		}
	}
	if !found1 || !found2 {
		t.Fatalf("Expected both meta tag records to be found, but at least one wasn't: %t / %t", found1, found2)
	}

	id, record, oldId, oldRecord, err := metaTagRecords.upsert(recordToUpdate)
	if err != nil {
		t.Fatalf("Expected no error, but there was one: %q", err)
	}
	if record == nil {
		t.Fatalf("Expected record to not be nil, but it was")
	}
	if recordIdToUpdate != id {
		t.Fatalf("Expected the new id after updating to be %d (the id that got returned when creating the record), but it was %d", recordIdToUpdate, id)
	}
	if oldId != id {
		t.Fatalf("Expected the new id after updating to be %d (same as the old id), but it was %d", oldId, id)
	}
	if oldRecord == nil {
		t.Fatalf("Expected the old record to not be nil, but it was")
	}

	if len(metaTagRecords) != 2 {
		t.Fatalf("Expected that there to be 2 meta tag records, but there were %d", len(metaTagRecords))
	}

	// the order of the records may have changed again due to sorting by id
	found1, found2 = false, false
	for id, record := range metaTagRecords {
		if len(record.Queries) != 2 {
			t.Fatalf("Expected every record to have 2 queries, but one had not: %+v", record)
		}
		switch record.Queries[0].Value {
		case "^(?:a)":
			record1 = metaTagRecords[id]
			found1 = true
		case "^(?:c)":
			record2 = metaTagRecords[id]
			found2 = true
		}
	}

	if !found1 || !found2 {
		t.Fatalf("Expected both meta tag records to be found, but not both were: %t / %t", found1, found2)
	}

	expectedRecord1 := tagquery.MetaTagRecord{
		MetaTags: []tagquery.Tag{
			{
				Key:   "metaTag1",
				Value: "value2",
			},
		},
		Queries: tagquery.Expressions{
			tagquery.Expression{
				Tag: tagquery.Tag{
					Key:   "tag1",
					Value: "^(?:a)",
				},
				Operator:              tagquery.MATCH,
				RequiresNonEmptyValue: true,
				UsesRegex:             true,
			},
			tagquery.Expression{
				Tag: tagquery.Tag{
					Key:   "tag2",
					Value: "^(?:b)",
				},
				Operator:              tagquery.MATCH,
				RequiresNonEmptyValue: true,
				UsesRegex:             true,
			},
		},
	}

	// ignore the compiled regex structs, as they can't reliably be compared
	for i := range record1.Queries {
		record1.Queries[i].Regex = nil
	}
	if !reflect.DeepEqual(record1, expectedRecord1) {
		t.Fatalf("Record1 did not look as expected:\nExpected\n%+v\nGot:\n%+v", expectedRecord1, record1)
	}

	expectedRecord2 := tagquery.MetaTagRecord{
		MetaTags: []tagquery.Tag{
			{
				Key:   "metaTag1",
				Value: "value1",
			},
		},
		Queries: tagquery.Expressions{
			tagquery.Expression{
				Tag: tagquery.Tag{
					Key:   "tag1",
					Value: "^(?:c)",
				},
				Operator:              tagquery.MATCH,
				RequiresNonEmptyValue: true,
				UsesRegex:             true,
			},
			tagquery.Expression{
				Tag: tagquery.Tag{
					Key:   "tag2",
					Value: "^(?:d)",
				},
				Operator:              tagquery.MATCH,
				RequiresNonEmptyValue: true,
				UsesRegex:             true,
			},
		},
	}

	// ignore the compiled regex structs, as they can't reliably be compared
	for i := range record2.Queries {
		record2.Queries[i].Regex = nil
	}
	if !reflect.DeepEqual(record2, expectedRecord2) {
		t.Fatalf("Record1 did not look as expected:\nExpected\n%+v\nGot:\n%+v", expectedRecord2, record2)
	}
}

// we mock the hashing algorithm implementation because we want to be able to
// test a hash collision
type mockHash struct {
	returnValues []uint32
	position     int
}

func (m *mockHash) Sum32() uint32 {
	value := m.returnValues[m.position%len(m.returnValues)]
	m.position = (m.position + 1) % len(m.returnValues)
	return value
}

func (m *mockHash) Write(_ []byte) (n int, err error) {
	return
}

func (m *mockHash) Sum(_ []byte) (res []byte) {
	return
}

func (m *mockHash) Reset() {}

func (m *mockHash) Size() (n int) {
	return
}
func (m *mockHash) BlockSize() (n int) {
	return
}

// We set the hash collision window to 3, so up to 3 hash collisions are allowed per hash value
// When more than 3 hash collisions are encountered for one hash value, new records are rejected
func TestHashCollisionsOnInsert(t *testing.T) {
	originalCollisionAvoidanceWindow := collisionAvoidanceWindow
	defer func() { collisionAvoidanceWindow = originalCollisionAvoidanceWindow }()
	collisionAvoidanceWindow = 3

	originalHash := queryHash
	defer func() { queryHash = originalHash }()

	queryHash = func() hash.Hash32 {
		return &mockHash{
			returnValues: []uint32{1}, // keep returning 1
		}
	}

	metaTagRecords := make(metaTagRecords)
	record, _ := tagquery.ParseMetaTagRecord([]string{"metaTag1=value1"}, []string{"metricTag1=value1"})
	metaTagRecords.upsert(record)
	record, _ = tagquery.ParseMetaTagRecord([]string{"metaTag2=value2"}, []string{"metricTag2=value2"})
	metaTagRecords.upsert(record)
	record, _ = tagquery.ParseMetaTagRecord([]string{"metaTag3=value3"}, []string{"metricTag3=value3"})
	metaTagRecords.upsert(record)
	if len(metaTagRecords) != 3 {
		t.Fatalf("Expected 3 meta tag records to be present, but there were %d", len(metaTagRecords))
	}

	// adding a 4th record with the same hash but different queries
	record, _ = tagquery.ParseMetaTagRecord([]string{"metaTag4=value4"}, []string{"metricTag4=value4"})
	id, returnedRecord, oldId, oldRecord, err := metaTagRecords.upsert(record)
	if err == nil {
		t.Fatalf("Expected an error to be returned, but there was none")
	}
	if id != 0 {
		t.Fatalf("Expected the returned id to be 0, but it was %d", id)
	}
	if returnedRecord != nil {
		t.Fatalf("Expected returned record point to be nil, but it wasn't")
	}
	if oldId != 0 {
		t.Fatalf("Expected oldId to be 0, but it was %d", oldId)
	}
	if oldRecord != nil {
		t.Fatalf("Expected oldRecord to be nil, but it wasn't")
	}
	if len(metaTagRecords) != 3 {
		t.Fatalf("Expected 3 meta tag records to be present, but there were %d", len(metaTagRecords))
	}

	// updating the third record with the same hash and equal queries
	record, _ = tagquery.ParseMetaTagRecord([]string{"metaTag3=value4"}, []string{"metricTag3=value3"})
	id, returnedRecord, oldId, oldRecord, err = metaTagRecords.upsert(record)
	if err != nil {
		t.Fatalf("Expected no error, but there was one: %q", err)
	}
	if id != 3 {
		t.Fatalf("Expected the returned id to be 3, but it was %d", id)
	}

	// check if the returned new record looks as expected
	if !reflect.DeepEqual(returnedRecord, &record) {
		t.Fatalf("New record looked different than expected:\nExpected:\n%+v\nGot:\n%+v\n", &record, returnedRecord)
	}
	if oldId != 3 {
		t.Fatalf("Expected oldId to be 3, but it was %d", oldId)
	}

	// check if the returned old record looks as expected
	record, _ = tagquery.ParseMetaTagRecord([]string{"metaTag3=value3"}, []string{"metricTag3=value3"})
	if !reflect.DeepEqual(oldRecord, &record) {
		t.Fatalf("Old record looked different than expected:\nExpected:\n%+v\nGot:\n%+v\n", &record, oldRecord)
	}
	if len(metaTagRecords) != 3 {
		t.Fatalf("Expected 3 meta tag records to be present, but there were %d", len(metaTagRecords))
	}
}

func TestDeletingMetaRecord(t *testing.T) {
	// Adding 2 meta records
	metaTagRecords := make(metaTagRecords)
	record, _ := tagquery.ParseMetaTagRecord([]string{"metaTag1=value1"}, []string{"metricTag1=value1"})
	metaTagRecords.upsert(record)

	record, _ = tagquery.ParseMetaTagRecord([]string{"metaTag2=value2"}, []string{"metricTag2=value2"})
	idOfRecord2, _, _, _, _ := metaTagRecords.upsert(record)

	if len(metaTagRecords) != 2 {
		t.Fatalf("Expected that 2 meta tag records exist, but there were %d", len(metaTagRecords))
	}

	// then we delete one record again
	record.MetaTags = nil
	id, returnedRecord, oldId, _, err := metaTagRecords.upsert(record)
	if err != nil {
		t.Fatalf("Expected no error, but there was one: %q", err)
	}
	if len(returnedRecord.MetaTags) != 0 {
		t.Fatalf("Expected returned meta tag record to have 0 meta tags, but it had %d", len(returnedRecord.MetaTags))
	}
	if !reflect.DeepEqual(returnedRecord.Queries, record.Queries) {
		t.Fatalf("Queries of returned record don't match what we expected:\nExpected:\n%+v\nGot:\n%+v\n", record.Queries, returnedRecord.Queries)
	}
	if oldId != idOfRecord2 {
		t.Fatalf("Expected the oldId to be the id of record2 (%d), but it was %d", idOfRecord2, oldId)
	}
	if len(metaTagRecords) != 1 {
		t.Fatalf("Expected that there is 1 meta tag record, but there were %d", len(metaTagRecords))
	}
	_, ok := metaTagRecords[id]
	if ok {
		t.Fatalf("Expected returned record id to not be present, but it was")
	}
}
