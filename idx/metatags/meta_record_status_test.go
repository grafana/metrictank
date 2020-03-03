package metatags

import (
	"testing"
)

func TestInitializingMetaRecordStatus(t *testing.T) {
	status := NewMetaRecordStatusByOrg()
	batchId, err := RandomUUID()
	if err != nil {
		t.Fatalf("Unexpected error when generating random UUID: %s", err)
	}
	update, batchToUpdate := status.Update(1, batchId, 100, 100)
	if !update || batchToUpdate != batchId {
		t.Fatalf("Unexpected return values: %t / %s", update, batchToUpdate.String())
	}

	update, batchToUpdate = status.Update(1, batchId, 50, 50)
	if !update {
		t.Fatalf("Expected update to be true, but it was false")
	}
	if batchToUpdate != batchId {
		t.Fatalf("Expected batch to update to be %s, but it was %s", batchId.String(), batchToUpdate.String())
	}

	update, _ = status.Update(1, batchId, 50, 50)
	if update {
		t.Fatalf("Expected update to be false, but it was true")
	}

	if orgBatch, ok := status.byOrg[1]; !ok {
		t.Fatalf("Expected org 1 is not present in returned result")
	} else {
		if orgBatch.batchId != batchId {
			t.Fatalf("Unexpected batch id. Expected %s, got %s", batchId.String(), orgBatch.batchId.String())
		}
	}
}
