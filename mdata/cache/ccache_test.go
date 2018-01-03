package cache

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/grafana/metrictank/mdata/chunk"
	"github.com/grafana/metrictank/test"
)

// getItgen returns an IterGen which holds a chunk which has directly encoded all values
func getItgen(t *testing.T, values []uint32, ts uint32, spanaware bool) chunk.IterGen {
	var b []byte
	buf := new(bytes.Buffer)
	if spanaware {
		binary.Write(buf, binary.LittleEndian, uint8(chunk.FormatStandardGoTszWithSpan))
		spanCode, ok := chunk.RevChunkSpans[uint32(len(values))]
		if !ok {
			t.Fatalf("invalid chunk span provided (%d)", len(values))
		}
		binary.Write(buf, binary.LittleEndian, spanCode)
	} else {
		binary.Write(buf, binary.LittleEndian, uint8(chunk.FormatStandardGoTsz))
	}
	for _, val := range values {
		binary.Write(buf, binary.LittleEndian, uint32(val))
	}
	buf.Write(b)

	itgen, _ := chunk.NewGen(buf.Bytes(), ts)

	return *itgen
}

func getConnectedChunks(t *testing.T, metric string) *CCache {
	cc := NewCCache()

	values := []uint32{1, 2, 3, 4, 5}
	itgen1 := getItgen(t, values, 1000, false)
	itgen2 := getItgen(t, values, 1005, false)
	itgen3 := getItgen(t, values, 1010, false)
	itgen4 := getItgen(t, values, 1015, false)
	itgen5 := getItgen(t, values, 1020, false)

	cc.Add(metric, metric, 0, itgen1)
	cc.Add(metric, metric, 1000, itgen2)
	cc.Add(metric, metric, 1005, itgen3)
	cc.Add(metric, metric, 1010, itgen4)
	cc.Add(metric, metric, 1015, itgen5)

	return cc
}

// test AddIfHot method without passing a previous timestamp on a hot metric
func TestAddIfHotWithoutPrevTsOnHotMetric(t *testing.T) {
	metric := "metric1"
	cc := NewCCache()

	values := []uint32{1, 2, 3, 4, 5}
	itgen1 := getItgen(t, values, 1000, false)
	itgen2 := getItgen(t, values, 1005, false)
	itgen3 := getItgen(t, values, 1010, false)

	cc.Add(metric, metric, 0, itgen1)
	cc.Add(metric, metric, 1000, itgen2)

	cc.CacheIfHot(metric, 0, itgen3)

	mc := cc.metricCache[metric]

	chunk, ok := mc.chunks[1010]
	if !ok {
		t.Fatalf("expected cache chunk to have been cached")
	}

	if itgen3.Ts != chunk.Ts {
		t.Fatalf("cached chunk wasn't the expected one")
	}

	if chunk.Prev != 1005 {
		t.Fatalf("expected cache chunk's previous ts to be 1005, but got %d", chunk.Prev)
	}

	if mc.chunks[chunk.Prev].Next != chunk.Ts {
		t.Fatalf("previous cache chunk didn't point at this one as it's next, got %d", mc.chunks[chunk.Prev].Next)
	}
}

// test AddIfHot method without passing a previous timestamp on a cold metric
func TestAddIfHotWithoutPrevTsOnColdMetric(t *testing.T) {
	metric := "metric1"
	cc := NewCCache()

	values := []uint32{1, 2, 3, 4, 5}
	itgen1 := getItgen(t, values, 1000, false)
	itgen3 := getItgen(t, values, 1010, false)

	cc.Add(metric, metric, 0, itgen1)

	cc.CacheIfHot(metric, 0, itgen3)

	mc := cc.metricCache[metric]

	_, ok := mc.chunks[1010]
	if ok {
		t.Fatalf("expected cache chunk to not have been cached")
	}

	if mc.chunks[1000].Next != 0 {
		t.Fatalf("previous cache chunk got wrongly connected with a following one, got %d", mc.chunks[1000].Next)
	}
}

// test AddIfHot method on a hot metric
func TestAddIfHotWithPrevTsOnHotMetric(t *testing.T) {
	metric := "metric1"
	cc := NewCCache()

	values := []uint32{1, 2, 3, 4, 5}
	itgen1 := getItgen(t, values, 1000, false)
	itgen2 := getItgen(t, values, 1005, false)
	itgen3 := getItgen(t, values, 1010, false)

	cc.Add(metric, metric, 0, itgen1)
	cc.Add(metric, metric, 1000, itgen2)

	cc.CacheIfHot(metric, 1005, itgen3)

	mc := cc.metricCache[metric]

	chunk, ok := mc.chunks[1010]
	if !ok {
		t.Fatalf("expected cache chunk to have been cached")
	}

	if itgen3.Ts != chunk.Ts {
		t.Fatalf("cached chunk wasn't the expected one")
	}

	if chunk.Prev != 1005 {
		t.Fatalf("expected cache chunk's previous ts to be 1005, but got %d", chunk.Prev)
	}

	if mc.chunks[chunk.Prev].Next != chunk.Ts {
		t.Fatalf("previous cache chunk didn't point at this one as it's next, got %d", mc.chunks[chunk.Prev].Next)
	}
}

// test AddIfHot method on a cold metric
func TestAddIfHotWithPrevTsOnColdMetric(t *testing.T) {
	metric := "metric1"
	cc := NewCCache()

	values := []uint32{1, 2, 3, 4, 5}
	itgen1 := getItgen(t, values, 1000, false)
	itgen3 := getItgen(t, values, 1010, false)

	cc.Add(metric, metric, 0, itgen1)

	cc.CacheIfHot(metric, 1005, itgen3)

	mc := cc.metricCache[metric]

	_, ok := mc.chunks[1010]
	if ok {
		t.Fatalf("expected cache chunk to not have been cached")
	}

	if mc.chunks[1000].Next != 0 {
		t.Fatalf("previous cache chunk got wrongly connected with a following one, got %d", mc.chunks[1000].Next)
	}
}

func TestConsecutiveAdding(t *testing.T) {
	metric := "metric1"
	cc := NewCCache()

	values := []uint32{1, 2, 3, 4, 5}
	itgen1 := getItgen(t, values, 1000, false)
	itgen2 := getItgen(t, values, 1005, false)

	cc.Add(metric, metric, 0, itgen1)
	cc.Add(metric, metric, 1000, itgen2)

	mc := cc.metricCache[metric]
	chunk1, ok := mc.chunks[1000]
	if !ok {
		t.Fatalf("expected cache chunk 1000 not found")
	}
	chunk2, ok := mc.chunks[1005]
	if !ok {
		t.Fatalf("expected cache chunk 2000 not found")
	}

	if chunk1.Prev != 0 {
		t.Fatalf("Expected previous chunk to be 0, got %d", chunk1.Prev)
	}
	if chunk1.Next != 1005 {
		t.Fatalf("Expected next chunk to be 2000, got %d", chunk1.Next)
	}
	if chunk2.Prev != 1000 {
		t.Fatalf("Expected previous chunk to be 1000, got %d", chunk2.Prev)
	}
	if chunk2.Next != 0 {
		t.Fatalf("Expected next chunk to be 0, got %d", chunk2.Next)
	}
}

// tests if chunks get connected to previous even if it is is not specified, based on span
func TestDisconnectedAdding(t *testing.T) {
	metric := "metric1"
	cc := NewCCache()

	values := []uint32{1, 2, 3, 4, 5}
	itgen1 := getItgen(t, values, 1000, true)
	itgen2 := getItgen(t, values, 1005, true)
	itgen3 := getItgen(t, values, 1010, true)

	cc.Add(metric, metric, 0, itgen1)
	cc.Add(metric, metric, 0, itgen2)
	cc.Add(metric, metric, 0, itgen3)

	res := cc.Search(test.NewContext(), metric, 900, 1015)

	if res.Complete {
		t.Fatalf("complete is expected to be false")
	}

	if len(res.Start) != 0 {
		t.Fatalf("expected to get 0 itergens in Start, got %d", len(res.Start))
	}

	if len(res.End) != 3 {
		t.Fatalf("expected to get 3 itergens in End, got %d", len(res.End))
	}

	if res.End[0].Ts != 1010 || res.End[len(res.End)-1].Ts != 1000 {
		t.Fatalf("result set is wrong")
	}
}

// tests if chunks get connected to previous even if it is is not specified,
// basesd on a span which is the result of a guess that's based on the distance to the previous chunk
func TestDisconnectedAddingByGuessing(t *testing.T) {
	metric := "metric1"
	cc := NewCCache()

	values := []uint32{1, 2, 3, 4, 5}
	itgen1 := getItgen(t, values, 1000, false)
	itgen2 := getItgen(t, values, 1005, false)
	itgen3 := getItgen(t, values, 1010, false)

	cc.Add(metric, metric, 0, itgen1)
	cc.Add(metric, metric, 1000, itgen2)
	cc.Add(metric, metric, 0, itgen3)

	res := cc.Search(test.NewContext(), metric, 900, 1015)

	if res.Complete {
		t.Fatalf("complete is expected to be false")
	}

	if len(res.Start) != 0 {
		t.Fatalf("expected to get 0 itergens in Start, got %d", len(res.Start))
	}

	if len(res.End) != 3 {
		t.Fatalf("expected to get 3 itergens in End, got %d", len(res.End))
	}

	if res.End[0].Ts != 1010 || res.End[len(res.End)-1].Ts != 1000 {
		t.Fatalf("result set is wrong")
	}

	mc, ok := cc.metricCache[metric]
	if !ok {
		t.Fatalf("cannot find metric that should be present")
	}

	lastChunk, ok := mc.chunks[1010]
	if !ok {
		t.Fatalf("cannot find chunk that should be present")
	}

	if lastChunk.Prev != 1005 {
		t.Fatalf("Add() method failed to correctly guess previous chunk")
	}
}

func TestSearchFromBeginningComplete(t *testing.T) {
	metric := "metric1"
	cc := getConnectedChunks(t, metric)
	res := cc.Search(test.NewContext(), metric, 1006, 1025)

	if !res.Complete {
		t.Fatalf("complete is expected to be true")
	}

	if len(res.Start) != 4 {
		t.Fatalf("expected to get 4 itergens, got %d", len(res.Start))
	}

	if res.Start[0].Ts != 1005 || res.Start[len(res.Start)-1].Ts != 1020 {
		t.Fatalf("result set is wrong")
	}
}

func TestSearchFromBeginningIncompleteEnd(t *testing.T) {
	metric := "metric1"
	cc := getConnectedChunks(t, metric)
	res := cc.Search(test.NewContext(), metric, 1006, 1030)
	if res.Complete {
		t.Fatalf("complete is expected to be false")
	}

	if len(res.Start) != 4 {
		t.Fatalf("expected to get 4 itergens, got %d", len(res.Start))
	}

	if res.Start[0].Ts != 1005 || res.Start[len(res.Start)-1].Ts != 1020 {
		t.Fatalf("result set is wrong")
	}
}

func TestSearchFromEnd(t *testing.T) {
	metric := "metric1"
	cc := getConnectedChunks(t, metric)
	res := cc.Search(test.NewContext(), metric, 500, 1025)

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

	if res.End[0].Ts != 1020 || res.End[len(res.End)-1].Ts != 1000 {
		t.Fatalf("result set is wrong")
	}
}

func TestSearchDisconnectedStartEndSpanawareAscending(t *testing.T) {
	testSearchDisconnectedStartEnd(t, true, true)
}

func TestSearchDisconnectedStartEndSpanawareDescending(t *testing.T) {
	testSearchDisconnectedStartEnd(t, true, false)
}

func TestSearchDisconnectedStartEndNonSpanaware(t *testing.T) {
	testSearchDisconnectedStartEnd(t, false, true)
}

func testSearchDisconnectedStartEnd(t *testing.T, spanaware, ascending bool) {
	var cc *CCache
	var res *CCSearchResult
	metric := "metric1"
	values := []uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}

	itgen1 := getItgen(t, values, 1000, spanaware)
	itgen2 := getItgen(t, values, 1010, spanaware)
	itgen3 := getItgen(t, values, 1020, spanaware)
	itgen4 := getItgen(t, values, 1030, spanaware)
	itgen5 := getItgen(t, values, 1040, spanaware)
	itgen6 := getItgen(t, values, 1050, spanaware)
	cc = NewCCache()

	for from := uint32(1000); from < 1010; from++ {
		// the end of ranges is exclusive, so we go up to 1060
		for until := uint32(1051); until < 1061; until++ {
			cc.Reset()

			if ascending {
				cc.Add(metric, metric, 0, itgen1)
				cc.Add(metric, metric, 1000, itgen2)
				cc.Add(metric, metric, 1010, itgen3)
				cc.Add(metric, metric, 0, itgen4)
				cc.Add(metric, metric, 1030, itgen5)
				cc.Add(metric, metric, 1040, itgen6)
			} else {
				cc.Add(metric, metric, 0, itgen6)
				cc.Add(metric, metric, 0, itgen5)
				cc.Add(metric, metric, 0, itgen4)
				cc.Add(metric, metric, 0, itgen3)
				cc.Add(metric, metric, 0, itgen2)
				cc.Add(metric, metric, 0, itgen1)
			}

			res = cc.Search(test.NewContext(), metric, from, until)
			if !res.Complete {
				t.Fatalf("from %d, until %d: complete is expected to be true", from, until)
			}

			if len(res.Start) != 6 {
				t.Fatalf("from %d, until %d: expected to get %d itergens at start, got %d", from, until, 6, len(res.Start))
			}

			if res.Start[0].Ts != 1000 || res.Start[len(res.Start)-1].Ts != 1050 {
				t.Fatalf("from %d, until %d: result set at Start is wrong", from, until)
			}

			if res.From != 1060 {
				t.Fatalf("from %d, until %d: expected From to be %d, got %d", from, until, 1060, res.From)
			}

			if len(res.End) != 0 {
				t.Fatalf("from %d, until %d: expected to get %d itergens at end, got %d", from, until, 0, len(res.End))
			}

			if res.Until != until {
				t.Fatalf("from %d, until %d: expected Until to be %d, got %d", from, until, 1055, res.Until)
			}
		}
	}
}

func TestSearchDisconnectedWithGapStartEndSpanawareAscending(t *testing.T) {
	testSearchDisconnectedWithGapStartEnd(t, true, true)
}

func TestSearchDisconnectedWithGapStartEndSpanawareDescending(t *testing.T) {
	testSearchDisconnectedWithGapStartEnd(t, true, false)
}

func TestSearchDisconnectedWithGapStartEndNonSpanaware(t *testing.T) {
	testSearchDisconnectedWithGapStartEnd(t, false, true)
}

func testSearchDisconnectedWithGapStartEnd(t *testing.T, spanaware, ascending bool) {
	metric := "metric1"
	var cc *CCache
	var res *CCSearchResult

	values := []uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	itgen1 := getItgen(t, values, 1000, spanaware)
	itgen2 := getItgen(t, values, 1010, spanaware)
	itgen3 := getItgen(t, values, 1020, spanaware)
	// missing chunk
	itgen4 := getItgen(t, values, 1040, spanaware)
	itgen5 := getItgen(t, values, 1050, spanaware)
	itgen6 := getItgen(t, values, 1060, spanaware)
	cc = NewCCache()

	for from := uint32(1000); from < 1010; from++ {
		// the end of ranges is exclusive, so we go up to 1060
		for until := uint32(1061); until < 1071; until++ {
			cc.Reset()

			if ascending {
				cc.Add(metric, metric, 0, itgen1)
				cc.Add(metric, metric, 1000, itgen2)
				cc.Add(metric, metric, 1010, itgen3)
				cc.Add(metric, metric, 0, itgen4)
				cc.Add(metric, metric, 1040, itgen5)
				cc.Add(metric, metric, 1050, itgen6)
			} else {
				cc.Add(metric, metric, 0, itgen6)
				cc.Add(metric, metric, 0, itgen5)
				cc.Add(metric, metric, 0, itgen4)
				cc.Add(metric, metric, 0, itgen3)
				cc.Add(metric, metric, 0, itgen2)
				cc.Add(metric, metric, 0, itgen1)
			}

			res = cc.Search(test.NewContext(), metric, from, until)
			if res.Complete {
				t.Fatalf("from %d, until %d: complete is expected to be false", from, until)
			}

			if len(res.Start) != 3 {
				t.Fatalf("from %d, until %d: expected to get 3 itergens at start, got %d", from, until, len(res.Start))
			}

			if res.Start[0].Ts != 1000 || res.Start[len(res.Start)-1].Ts != 1020 {
				t.Fatalf("from %d, until %d: result set at Start is wrong", from, until)
			}

			if res.From != 1030 {
				t.Fatalf("from %d, until %d: expected From to be %d but got %d", from, until, 1030, res.From)
			}

			if len(res.End) != 3 {
				t.Fatalf("from %d, until %d: expected to get 3 itergens at end, got %d", from, until, len(res.End))
			}

			if res.End[0].Ts != 1060 || res.End[len(res.End)-1].Ts != 1040 {
				t.Fatalf("from %d, until %d: result set at End is wrong", from, until)
			}

			if res.Until != 1040 {
				t.Fatalf("from %d, until %d: expected Until to be %d but got %d", from, until, 1030, res.Until)
			}
		}
	}
}

func TestReset(t *testing.T) {
	cc := NewCCache()

	// .Reset() is being called at the end of testMetricDelete()
	testMetricDelete(t, cc)
	testMetricDelete(t, cc)
	testMetricDelete(t, cc)
}

func TestMetricDelete(t *testing.T) {
	cc := NewCCache()
	testMetricDelete(t, cc)
}

func testMetricDelete(t *testing.T, cc *CCache) {
	var res *CCSearchResult

	rawMetric1 := "some.tree.metric1"
	metric1_1 := "some.tree.metric1_600_cnt"
	metric1_2 := "some.tree.metric1_600_sum"
	rawMetric2 := "some.tree.metric2"
	metric2_1 := "some.tree.metric2_6000_cnt"
	values := []uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	itgenCount := 10
	itgens := make([]chunk.IterGen, 0, itgenCount)
	for i := 1000; i < 1000+itgenCount*len(values); i = i + len(values) {
		itgens = append(itgens, getItgen(t, values, uint32(i), true))
	}

	// add two metrics with itgenCount chunks each
	for _, itgen := range itgens {
		cc.Add(metric1_1, rawMetric1, 0, itgen)
		cc.Add(metric1_2, rawMetric1, 0, itgen)
		cc.Add(metric2_1, rawMetric2, 0, itgen)
	}

	// check if Search returns them all for metric1_1
	res = cc.Search(test.NewContext(), metric1_1, 1000, uint32(1000+itgenCount*len(values)))
	if len(res.Start) != itgenCount {
		t.Fatalf("Expected to have %d values, got %d", itgenCount, len(res.Start))
	}

	// check if Search returns them all for metric1_2
	res = cc.Search(test.NewContext(), metric1_2, 1000, uint32(1000+itgenCount*len(values)))
	if len(res.Start) != itgenCount {
		t.Fatalf("Expected to have %d values, got %d", itgenCount, len(res.Start))
	}

	// check if Search returns them all for metric2_1
	res = cc.Search(test.NewContext(), metric2_1, 1000, uint32(1000+itgenCount*len(values)))
	if len(res.Start) != itgenCount {
		t.Fatalf("Expected to have %d values, got %d", itgenCount, len(res.Start))
	}

	// now delete metric1_1, but leave metric2_1
	delRes := cc.DelMetric(rawMetric1)
	expectDelSeries := 1
	if delRes.Series != expectDelSeries {
		t.Fatalf("Expected exactly %d series to get deleted, but got %d", expectDelSeries, delRes.Series)
	}

	expectDelArchives := 2
	if delRes.Archives != expectDelArchives {
		t.Fatalf("Expected exactly %d archives to get deleted, but got %d", expectDelArchives, delRes.Archives)
	}

	// check if metric1_1 returns no results anymore
	res = cc.Search(test.NewContext(), metric1_1, 1000, uint32(1000+itgenCount*len(values)))
	if len(res.Start) != 0 {
		t.Fatalf("Expected to have %d values, got %d", 0, len(res.Start))
	}

	// check if metric1_1 returns no results anymore
	res = cc.Search(test.NewContext(), metric1_2, 1000, uint32(1000+itgenCount*len(values)))
	if len(res.Start) != 0 {
		t.Fatalf("Expected to have %d values, got %d", 0, len(res.Start))
	}

	// but metric2_1 should still be there
	res = cc.Search(test.NewContext(), metric2_1, 1000, uint32(1000+itgenCount*len(values)))
	if len(res.Start) != itgenCount {
		t.Fatalf("Expected to have %d values, got %d", itgenCount, len(res.Start))
	}

	// now add metric1_1 and metric1_2 again
	for _, itgen := range itgens {
		cc.Add(metric1_1, rawMetric1, 0, itgen)
		cc.Add(metric1_2, rawMetric1, 0, itgen)
	}

	// and check if it gets returned by Search again
	res = cc.Search(test.NewContext(), metric1_1, 1000, uint32(1000+itgenCount*len(values)))
	if len(res.Start) != itgenCount {
		t.Fatalf("Expected to have %d values, got %d", itgenCount, len(res.Start))
	}

	// now reset the whole metric cache
	delRes = cc.Reset()
	expectDelSeries = 2
	if delRes.Series != expectDelSeries {
		t.Fatalf("Expected exactly %d series to get deleted, but got %d", expectDelSeries, delRes.Series)
	}

	expectDelArchives = 3
	if delRes.Archives != expectDelArchives {
		t.Fatalf("Expected exactly %d archives to get deleted, but got %d", expectDelArchives, delRes.Archives)
	}
}
