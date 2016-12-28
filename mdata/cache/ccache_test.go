package cache

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/raintank/metrictank/mdata/chunk"
)

func getItgen(value uint32, ts uint32) *chunk.IterGen {
	var b []byte
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, uint8(chunk.FormatStandardGoTsz))
	binary.Write(buf, binary.LittleEndian, uint32(value))
	buf.Write(b)

	itgen, _ := chunk.NewGen(buf.Bytes(), ts)

	return itgen
}

func getConnectedChunks(metric string) Cache {
	cc := NewCCache()

	itgen1 := getItgen(1111111, 1000)
	itgen2 := getItgen(1111111, 2000)
	itgen3 := getItgen(1111111, 3000)
	itgen4 := getItgen(1111111, 4000)
	itgen5 := getItgen(1111111, 5000)

	cc.Add(metric, 0, *itgen1)
	cc.Add(metric, 1000, *itgen2)
	cc.Add(metric, 2000, *itgen3)
	cc.Add(metric, 3000, *itgen4)
	cc.Add(metric, 4000, *itgen5)

	return cc
}

func TestConsecutiveAdding(t *testing.T) {
	metric := "metric1"
	cc := NewCCache()

	itgen1 := getItgen(1111111, 1000)
	itgen2 := getItgen(1111111, 2000)

	cc.Add(metric, 0, *itgen1)
	cc.Add(metric, 1000, *itgen2)

	mc := cc.metricCache[metric]
	chunk1, ok := mc.chunks[1000]
	if !ok {
		t.Fatalf("expected cache chunk 1000 not found")
	}
	chunk2, ok := mc.chunks[2000]
	if !ok {
		t.Fatalf("expected cache chunk 2000 not found")
	}

	if chunk1.Prev != 0 {
		t.Fatalf("Expected previous chunk to be 0, got %d", chunk1.Prev)
	}
	if chunk1.Next != 2000 {
		t.Fatalf("Expected next chunk to be 2000, got %d", chunk1.Next)
	}
	if chunk2.Prev != 1000 {
		t.Fatalf("Expected previous chunk to be 1000, got %d", chunk2.Prev)
	}
	if chunk2.Next != 0 {
		t.Fatalf("Expected next chunk to be 0, got %d", chunk2.Next)
	}
}

func TestSearchFromBeginningComplete(t *testing.T) {
	metric := "metric1"
	cc := getConnectedChunks(metric)
	res := cc.Search(metric, 2500, 5999)

	if !res.Complete {
		t.Fatalf("complete is expected to be true")
	}

	if len(res.Start) != 4 {
		t.Fatalf("expected to get 4 itergens, got %d", len(res.Start))
	}

	if res.Start[0].Ts() != 2000 || res.Start[len(res.Start)-1].Ts() != 5000 {
		t.Fatalf("result set is wrong")
	}
}

func TestSearchFromBeginningIncompleteEnd(t *testing.T) {
	metric := "metric1"
	cc := getConnectedChunks(metric)
	res := cc.Search(metric, 2500, 6000)
	if !res.Complete {
		t.Fatalf("complete is expected to be false")
	}

	if len(res.Start) != 4 {
		t.Fatalf("expected to get 4 itergens, got %d", len(res.Start))
	}

	if res.Start[0].Ts() != 2000 || res.Start[len(res.Start)-1].Ts() != 5000 {
		t.Fatalf("result set is wrong")
	}
}

func TestSearchFromEnd(t *testing.T) {
	metric := "metric1"
	cc := getConnectedChunks(metric)
	res := cc.Search(metric, 500, 5999)

	if res.Complete {
		t.Fatalf("complete is expected to not be true")
	}

	if res.From != 500 {
		t.Fatalf("From is expected to remain the original value")
	}

	if len(res.End) != 5 {
		t.Fatalf("expected to get 5 itergens, got %d", len(res.End))
	}

	if res.Until != 1000 {
		t.Fatalf("Until is expected to be 1000, got %d", res.Until)
	}

	if res.End[0].Ts() != 5000 || res.End[len(res.End)-1].Ts() != 1000 {
		t.Fatalf("result set is wrong")
	}
}
