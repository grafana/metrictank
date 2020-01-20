package metatags

import (
	"crypto/rand"
	"fmt"
	"io"
)

var (
	// this batch id is used if we handle an upsert request for an org that has
	// no current batch
	DefaultBatchId UUID

	ErrMetaTagSupportDisabled = fmt.Errorf("Meta tag support is not enabled")
)

type MetaRecordStatusByOrg struct {
	byOrg map[uint32]metaRecordStatus
}

func NewMetaRecordStatusByOrg() MetaRecordStatusByOrg {
	return MetaRecordStatusByOrg{byOrg: make(map[uint32]metaRecordStatus)}
}

type metaRecordStatus struct {
	batchId    UUID
	createdAt  uint64
	lastUpdate uint64
}

// UUIDs are used as meta record batch IDs
type UUID [16]byte

// RandomUUID generates a randomized UUID,
// the code is copied from gocql.RandomUUID()
func RandomUUID() (UUID, error) {
	var u UUID
	_, err := io.ReadFull(rand.Reader, u[:])
	if err != nil {
		return u, err
	}
	u[6] &= 0x0F // clear version
	u[6] |= 0x40 // set version to 4 (random uuid)
	u[8] &= 0x3F // clear variant
	u[8] |= 0x80 // set to IETF variant
	return u, nil
}

// ParseUUID parses a 32 digit hexadecimal number (that might contain hypens)
// representing an UUID.
func ParseUUID(input string) (UUID, error) {
	var u UUID
	j := 0
	for _, r := range input {
		switch {
		case r == '-' && j&1 == 0:
			continue
		case r >= '0' && r <= '9' && j < 32:
			u[j/2] |= byte(r-'0') << uint(4-j&1*4)
		case r >= 'a' && r <= 'f' && j < 32:
			u[j/2] |= byte(r-'a'+10) << uint(4-j&1*4)
		case r >= 'A' && r <= 'F' && j < 32:
			u[j/2] |= byte(r-'A'+10) << uint(4-j&1*4)
		default:
			return UUID{}, fmt.Errorf("invalid UUID %q", input)
		}
		j += 1
	}
	if j != 32 {
		return UUID{}, fmt.Errorf("invalid UUID %q", input)
	}
	return u, nil
}

// String returns the UUID in it's canonical form, a 32 digit hexadecimal
// number in the form of xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx.
func (u UUID) String() string {
	var offsets = [...]int{0, 2, 4, 6, 9, 11, 14, 16, 19, 21, 24, 26, 28, 30, 32, 34}
	const hexString = "0123456789abcdef"
	r := make([]byte, 36)
	for i, b := range u {
		r[offsets[i]] = hexString[b>>4]
		r[offsets[i]+1] = hexString[b&0xF]
	}
	r[8] = '-'
	r[13] = '-'
	r[18] = '-'
	r[23] = '-'
	return string(r)
}

// update takes the properties describing a batch of meta records and updates its internal status if necessary
// it returns a boolean indicating whether a reload of the meta records is necessary and
// if it is then the second returned value is the batch id that needs to be loaded
func (m *MetaRecordStatusByOrg) Update(orgId uint32, newBatch UUID, newCreatedAt, newLastUpdate uint64) (bool, UUID) {
	status, ok := m.byOrg[orgId]
	if !ok {
		m.byOrg[orgId] = metaRecordStatus{
			batchId:    newBatch,
			createdAt:  newCreatedAt,
			lastUpdate: newLastUpdate,
		}
		return true, newBatch
	}

	// if the current batch has been created at a time before the new batch,
	// then we want to make the new batch the current one and load its records
	if status.batchId != newBatch && status.createdAt < newCreatedAt {
		status.batchId = newBatch
		status.createdAt = newCreatedAt
		status.lastUpdate = newLastUpdate
		m.byOrg[orgId] = status
		return true, status.batchId
	}

	// if the current batch is the same as the new batch, but their last update times
	// differ, then we want to reload that batch
	if status.batchId == newBatch && status.lastUpdate != newLastUpdate {
		status.lastUpdate = newLastUpdate
		m.byOrg[orgId] = status
		return true, status.batchId
	}

	return false, DefaultBatchId
}

func (m *MetaRecordStatusByOrg) GetStatus(orgId uint32) (UUID, uint64, uint64) {
	status, ok := m.byOrg[orgId]
	if !ok {
		return DefaultBatchId, 0, 0
	}

	return status.batchId, status.createdAt, status.lastUpdate
}
