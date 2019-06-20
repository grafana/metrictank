package mdata

import (
	"bytes"
	"reflect"
	"testing"
	"time"

	"github.com/grafana/metrictank/mdata/chunk"
	"github.com/raintank/schema"
)

func getTestData(ts, interval, length uint32, val float64) []point {
	testData := make([]point, 0, length)

	for i := uint32(0); i < length; i++ {
		testData = append(testData, point{ts: ts, val: val})
		val++
		ts += interval
	}

	return testData
}

func chunksFromPoints(points []point, chunkSpan uint32) []*chunk.Chunk {
	var t0, prevT0 uint32
	var c *chunk.Chunk
	var encodedChunks []*chunk.Chunk

	for _, point := range points {
		t0 = point.ts - (point.ts % chunkSpan)

		if prevT0 == 0 {
			c = chunk.New(t0)
			prevT0 = t0
		} else if prevT0 != t0 {
			c.Finish()
			encodedChunks = append(encodedChunks, c)

			c = chunk.New(t0)
			prevT0 = t0
		}

		c.Push(point.ts, point.val)
	}

	c.Finish()
	encodedChunks = append(encodedChunks, c)

	return encodedChunks
}

func TestArchiveRequestEncodingDecoding(t *testing.T) {
	testData := getTestData(6000, 60, 100, 0)
	chunks := chunksFromPoints(testData, 1800)

	id := "98.12345678901234567890123456789012"
	key, err := schema.AMKeyFromString(id + "_sum_3600")
	if err != nil {
		t.Fatalf("Expected no error when getting AMKey from string %q", err)
	}
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
	}

	for i := 0; i < len(chunks); i++ {
		originalRequest.ChunkWriteRequests = append(originalRequest.ChunkWriteRequests, ChunkWriteRequest{
			Callback:  nil,
			Key:       key,
			TTL:       1,
			T0:        chunks[i].Series.T0,
			Data:      chunks[i].Encode(1800),
			Timestamp: time.Unix(123, 456),
		})
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

	var decodedData []point
	for _, c := range got.ChunkWriteRequests {
		itgen, err := chunk.NewIterGen(uint32(c.T0), 0, c.Data)
		if err != nil {
			t.Fatalf("Expected no error when getting IterGen %q", err)
		}

		iter, err := itgen.Get()
		if err != nil {
			t.Fatalf("Expected no error when getting Iterator %q", err)
		}

		for iter.Next() {
			ts, val := iter.Values()
			decodedData = append(decodedData, point{ts: ts, val: val})
		}
	}

	if !reflect.DeepEqual(testData, decodedData) {
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
