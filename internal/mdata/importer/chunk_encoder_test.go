package importer

import (
	"testing"

	"github.com/grafana/metrictank/internal/mdata/chunk"
	"github.com/kisielk/whisper-go/whisper"
)

func TestEncodedChunksFromPointsWithUnfinished(t *testing.T) {
	points := generatePoints(25200, 10, 10, 0, 8640, func(i float64) float64 { return i + 1 })
	expectedCount := 8640 // count including unfinished chunks

	chunks := encodeChunksFromPoints(points, 10, 21600, true)

	if len(chunks) != 5 {
		t.Fatalf("Expected to get 5 chunks, but got %d", len(chunks))
	}

	i := 0
	for _, c := range chunks {
		iterGen, err := chunk.NewIterGen(c.Series.T0, 10, c.Encode(21600))
		if err != nil {
			t.Fatalf("Error getting iterator: %s", err)
		}

		iter, err := iterGen.Get()
		if err != nil {
			t.Fatalf("Error getting iterator: %s", err)
		}

		for iter.Next() {
			ts, val := iter.Values()
			if points[i].Timestamp != ts || points[i].Value != val {
				t.Fatalf("Unexpected value at index %d:\nExpected: %d:%f\nGot: %d:%f\n", i, ts, val, points[i].Timestamp, points[i].Value)
			}
			i++
		}
	}
	if i != expectedCount {
		t.Fatalf("Unexpected number of datapoints in chunks:\nExpected: %d\nGot: %d\n", expectedCount, i)
	}
}

func TestEncodedChunksFromPointsWithoutUnfinished(t *testing.T) {
	// the actual data in these points doesn't matter, we just want to be sure
	// that the chunks resulting from these points include the same data
	points := generatePoints(25200, 10, 10, 0, 8640, func(i float64) float64 { return i + 1 })
	expectedCount := 8640 - (2520 % 2160) // count minus what would end up in an unfinished chunk

	chunks := encodeChunksFromPoints(points, 10, 21600, false)

	if len(chunks) != 4 {
		t.Fatalf("Expected to get 4 chunks, but got %d", len(chunks))
	}

	i := 0
	for _, c := range chunks {
		iterGen, err := chunk.NewIterGen(c.Series.T0, 10, c.Encode(21600))
		if err != nil {
			t.Fatalf("Error getting iterator: %s", err)
		}

		iter, err := iterGen.Get()
		if err != nil {
			t.Fatalf("Error getting iterator: %s", err)
		}

		for iter.Next() {
			ts, val := iter.Values()
			if points[i].Timestamp != ts || points[i].Value != val {
				t.Fatalf("Unexpected value at index %d:\nExpected: %d:%f\nGot: %d:%f\n", i, ts, val, points[i].Timestamp, points[i].Value)
			}
			i++
		}
	}
	if i != expectedCount {
		t.Fatalf("Unexpected number of datapoints in chunks:\nExpected: %d\nGot: %d\n", expectedCount, i)
	}
}

func generatePoints(ts, interval uint32, value float64, offset, count int, inc func(float64) float64) []whisper.Point {
	res := make([]whisper.Point, count)
	for i := 0; i < count; i++ {
		res[(i+offset)%count] = whisper.Point{
			Timestamp: ts,
			Value:     value,
		}
		ts += interval
		value = inc(value)
	}
	return res
}
