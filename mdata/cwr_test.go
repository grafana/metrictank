package mdata

import (
	"bytes"
	"reflect"
	"testing"
	"time"

	"github.com/raintank/schema"
)

func TestArchiveRequestEncodingDecoding(t *testing.T) {
	key, _ := schema.AMKeyFromString("98.12345678901234567890123456789012")
	originalRequest := ArchiveRequest{
		MetricData: schema.MetricData{
			Id:       "98.12345678901234567890123456789012",
			OrgId:    1,
			Name:     "testMetricData",
			Interval: 1,
			Value:    321,
			Unit:     "test",
			Time:     123456,
			Mtype:    "test",
			Tags:     []string{"some=tag"},
		},
		ChunkWriteRequests: []ChunkWriteRequest{
			{
				Callback:  nil,
				Key:       key,
				TTL:       1,
				T0:        2,
				Data:      []byte("abcdefghijklmnopqrstuvwxyz"),
				Timestamp: time.Unix(123, 456),
			},
		},
	}

	encoded, err := originalRequest.MarshalCompressed()
	if err != nil {
		t.Fatalf("Failed when encoding the request: %s", err)
	}

	got := ArchiveRequest{}
	err = got.UnmarshalCompressed(encoded)
	if err != nil {
		t.Fatalf("Expected no error when decoding request, got %q", err)
	}

	if !reflect.DeepEqual(originalRequest, got) {
		t.Fatalf("Decoded request is different than the encoded one.\nexp: %+v\ngot: %+v\n", originalRequest, got)
	}
}

func TestArchiveRequestEncodingWithUnknownVersion(t *testing.T) {
	originalRequest := ArchiveRequest{}
	encoded, err := originalRequest.MarshalCompressed()
	if err != nil {
		t.Fatalf("Expected no error when encoding request, got %q", err)
	}

	encodedBytes := encoded.Bytes()
	encodedBytes[0] = byte(uint8(2))
	decodedRequest := ArchiveRequest{}
	err = decodedRequest.UnmarshalCompressed(bytes.NewReader(encodedBytes))
	if err == nil {
		t.Fatalf("Expected an error when decoding, but got nil")
	}
}
