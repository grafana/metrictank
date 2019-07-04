package memory

import (
	"hash"
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

	if !metaTagRecordsAreEqual(&recordToInsert, record) {
		t.Fatalf("Inserted meta tag record has unexpectedly been modified")
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

	var found1, found2 bool
	var recordIdToUpdate recordId
	for i, record := range metaTagRecords {
		if metaTagRecordsAreEqual(&record, &recordToInsert1) {
			found1 = true
			recordIdToUpdate = i
		} else if metaTagRecordsAreEqual(&record, &recordToInsert2) {
			found2 = true
		}
	}

	if !(found1 && found2) {
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
	if oldRecord == nil || !metaTagRecordsAreEqual(oldRecord, &recordToInsert1) {
		t.Fatalf("Expected the old record to not be nil, but it was")
	}
	if len(metaTagRecords) != 2 {
		t.Fatalf("Expected that there to be 2 meta tag records, but there were %d", len(metaTagRecords))
	}

	// the order of the records may have changed again due to sorting by id
	found1, found2 = false, false
	for _, record := range metaTagRecords {
		if metaTagRecordsAreEqual(&record, &recordToUpdate) {
			found1 = true
		}
		if metaTagRecordsAreEqual(&record, &recordToInsert2) {
			found2 = true
		}
	}

	if !(found1 && found2) {
		t.Fatalf("Expected both meta tag records to be found, but not both were: %t / %t", found1, found2)
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

	// updating the third record with the same hash and equal queries, but different meta tags
	record, _ = tagquery.ParseMetaTagRecord([]string{"metaTag3=value4"}, []string{"metricTag3=value3"})
	id, returnedRecord, oldId, oldRecord, err = metaTagRecords.upsert(record)
	if err != nil {
		t.Fatalf("Expected no error, but there was one: %q", err)
	}
	if id != 3 {
		t.Fatalf("Expected the returned id to be 3, but it was %d", id)
	}

	// check if the returned new record looks as expected
	if !metaTagRecordsAreEqual(returnedRecord, &record) {
		t.Fatalf("New record looked different than expected:\nExpected:\n%+v\nGot:\n%+v\n", &record, returnedRecord)
	}
	if oldId != 3 {
		t.Fatalf("Expected oldId to be 3, but it was %d", oldId)
	}

	// check if the returned old record looks as expected
	record, _ = tagquery.ParseMetaTagRecord([]string{"metaTag3=value3"}, []string{"metricTag3=value3"})
	if !metaTagRecordsAreEqual(oldRecord, &record) {
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
	// upserting a meta tag record with one that has no meta tags results in deletion
	record.MetaTags = nil
	id, returnedRecord, oldId, _, err := metaTagRecords.upsert(record)
	if err != nil {
		t.Fatalf("Expected no error, but there was one: %q", err)
	}
	if len(returnedRecord.MetaTags) != 0 {
		t.Fatalf("Expected returned meta tag record to have 0 meta tags, but it had %d", len(returnedRecord.MetaTags))
	}
	if !metaTagRecordsAreEqual(returnedRecord, &record) {
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

func metaTagRecordsAreEqual(record1, record2 *tagquery.MetaTagRecord) bool {
	if len(record1.MetaTags) != len(record2.MetaTags) {
		return false
	}

	foundTags := make([]bool, len(record1.MetaTags))
	for i, tag := range record1.MetaTags {
		for _, otherTag := range record2.MetaTags {
			if tag == otherTag {
				foundTags[i] = true
			}
		}
	}

	for i := range foundTags {
		if !foundTags[i] {
			return false
		}
	}

	return record1.MatchesQueries(record2)
}
