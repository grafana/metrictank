package tsz

import (
	"bytes"
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"testing"

	"github.com/grafana/metrictank/pkg/schema"
	"github.com/grafana/metrictank/pkg/util"
)

type testCase struct {
	desc         string
	t0           uint32
	intervalHint uint32
	vals         []schema.Point
}

// skipEvery: -1 to disable skipping, otherwise every skipEvery valid points, skip a point
// skip: start counting pos (typically 0) but can be tweaked e.g. to start with a skip
func makeVals(from, to, delta uint32, startSkip, skipEvery int) []schema.Point {
	var out []schema.Point
	skip := startSkip

	for t := from; t < to; t += delta {
		if skip == skipEvery {
			skip = 0
			continue
		}
		out = append(out, schema.Point{
			Val: float64(t),
			Ts:  t,
		})
		skip += 1
	}
	return out
}

func TestSeriesEncodeDecode(t *testing.T) {
	// assume 6h chunks
	cases := []testCase{
		{
			"regular chunk with dense, 60s spread data",
			1540728000,
			60,
			makeVals(1540728000, 1540728000+3600*6, 60, 0, -1),
		},
		{
			"regular chunk with dense, 1s spread data",
			1540728000,
			1,
			makeVals(1540728000, 1540728000+3600*6, 1, 0, -1),
		},
		{
			"regular chunk with sparse, 60s spread data",
			1540728000,
			60,
			makeVals(1540728000, 1540728000+3600*6, 60, 0, 3),
		},
		{
			"regular chunk with sparse, 1s spread data",
			1540728000,
			1,
			makeVals(1540728000, 1540728000+3600*6, 1, 0, 3),
		},
		{
			"regular chunk with sparse, 60s spread data. start with a skip",
			1540728000,
			60,
			makeVals(1540728000, 1540728000+3600*6, 60, 3, 3),
		},
		{
			"regular chunk with sparse, 1s spread data. start with a skip",
			1540728000,
			1,
			makeVals(1540728000, 1540728000+3600*6, 1, 3, 3),
		},
		{
			"a single point in the beginning",
			1540728000,
			60,
			[]schema.Point{{1540728000, 1540728000}},
		},
		{
			"a single point (the 2nd point after to)",
			1540728000,
			60,
			[]schema.Point{{1540728000 + 60, 1540728000 + 60}},
		},
		{
			"a single point right at the end",
			1540728000,
			60,
			[]schema.Point{{1540749600 - 60, 1540749600 - 60}},
		},
		{
			"a single point in the beginning + one at the end",
			1540728000,
			60,
			[]schema.Point{{1540728000, 1540728000}, {1540749600 - 60, 1540749600 - 60}},
		},
		{
			"a point (the 2nd point after to) + one at the end",
			1540728000,
			60,
			[]schema.Point{{1540728000 + 60, 1540728000 + 60}, {1540749600 - 60, 1540749600 - 60}},
		},
		{
			"no data for 5 hours, then 1 hour of 60s dense data",
			1540728000,
			60,
			makeVals(1540728000+3600*5, 1540728000+3600*6, 60, 0, -1),
		},
		{
			"no data for 5 hours, then 1 hour of 60s sparse data",
			1540728000,
			60,
			makeVals(1540728000+3600*5, 1540728000+3600*6, 60, 0, 3),
		},
		{
			"no data for 5 hours, then 1 hour of 60s sparser data",
			1540728000,
			60,
			makeVals(1540728000+3600*5, 1540728000+3600*6, 60, 1, 1),
		},
	}

	for i, c := range cases {
		series4h := NewSeries4h(c.t0)
		seriesLong := NewSeriesLong(c.t0)
		for _, point := range c.vals {
			series4h.Push(point.Ts, point.Val)
			seriesLong.Push(point.Ts, point.Val)
		}
		series4h.Finish()
		seriesLong.Finish()
		bytes4h := series4h.Bytes()
		bytesLong := seriesLong.Bytes()

		// decode chunk.
		// note typically the storage system stores and retrieves the t0 along with the chunk data

		iter4h, err := NewIterator4h(bytes4h, c.intervalHint)
		if err != nil {
			t.Errorf("case %d: %s: could not get iterator for series4h: %s", i, c.desc, err)
		}
		iterLong, err := NewIteratorLong(c.t0, bytesLong)
		if err != nil {
			t.Errorf("case %d: %s: could not get iterator for seriesLong: %s", i, c.desc, err)
		}

		var out4h []schema.Point
		for iter4h.Next() {
			ts, val := iter4h.Values()
			out4h = append(out4h, schema.Point{
				Val: val,
				Ts:  ts,
			})
		}
		if !reflect.DeepEqual(c.vals, out4h) {
			t.Errorf("case %d: %s: decoded series4h does not match encoded data!\nexpected:\n%s\ngot:\n%s\n", i, c.desc, pretty(c.vals), pretty(out4h))
		}
		var outLong []schema.Point
		for iterLong.Next() {
			ts, val := iterLong.Values()
			outLong = append(outLong, schema.Point{
				Val: val,
				Ts:  ts,
			})
		}
		if !reflect.DeepEqual(c.vals, outLong) {
			t.Errorf("case %d: %s: decoded seriesLong does not match encoded data!\nexpected:\n%s\ngot:\n%s\n", i, c.desc, pretty(c.vals), pretty(outLong))
		}
	}
}

// TestSeriesLongEncodeDecodeRandom generates a bunch of random chunks
// and validates that what comes out, matches what went in
func TestSeriesLongEncodeDecodeRandom(t *testing.T) {
	for i := 0; i < 10000; i++ {
		testSeriesLongEncodeDecodeRandom(t)
	}
}

// explore as much of the problem space as possible:
func testSeriesLongEncodeDecodeRandom(t *testing.T) {
	// choose any of the chunkspans we support
	// and a random corresponding t0.
	chunkspan := ChunkSpans[rand.Int()%32]
	t0 := uint32(rand.Intn(int(math.MaxUint32 - chunkspan)))
	t0 -= (t0 % chunkspan)
	nextT0 := t0 + chunkspan
	prev := t0 - 1

	series := NewSeriesLong(t0)

	var in []schema.Point

	for {
		if prev == nextT0-1 {
			// we already added all the points we could add
			break
		}

		// choose an interval between two points
		maxDelta := nextT0 - prev - 1
		var delta uint32
		choice := rand.Intn(100)
		if choice < 25 {
			delta = 1
		} else if choice < 50 && maxDelta > 1 { // delta 2 ... 9
			delta = randUint32Range(2, util.Min(10, maxDelta+1))
		} else if choice <= 88 && maxDelta > 9 { // delta 10 ... (maxDelta - 5)
			delta = randUint32Range(10, maxDelta+1)
		} else if choice <= 98 && maxDelta > 9 { // delta (maxDelta-5) ... maxDelta
			delta = randUint32Range(maxDelta-5, maxDelta+1)
		} else {
			// 1% chance: bail out, unless the chunk hasn't received a single point yet
			if prev != t0-1 {
				break
			} else {
				continue
			}
		}

		ts := prev + delta
		if ts >= nextT0 {
			panic(fmt.Sprintf("this should never happen. ts >= nextT0"))
		}
		prev = ts

		var v float64
		switch rand.Intn(7) {
		case 0:
			v = 0
		case 1:
			v = 1
		case 2:
			v = math.MaxFloat64
		case 3:
			v = math.SmallestNonzeroFloat64
		case 4:
			v = math.MaxFloat64 * -1
		case 5:
			v = math.SmallestNonzeroFloat64 * -1
		case 6:
			v = rand.NormFloat64()
		}

		series.Push(ts, v)
		in = append(in, schema.Point{v, ts})
	}
	series.Finish()
	bytes := series.Bytes()

	// decode chunk.
	// note typically the storage system stores and retrieves the t0 along with the chunk data

	iter, err := NewIteratorLong(t0, bytes)
	if err != nil {
		t.Errorf("could not get iterator\ninput %v\nchunk %b\nerror: %s", in, bytes, err)
	}

	var out []schema.Point
	for iter.Next() {
		ts, val := iter.Values()
		out = append(out, schema.Point{
			Val: val,
			Ts:  ts,
		})
	}
	//t.Logf("testing chunk %v", in)
	if !reflect.DeepEqual(in, out) {
		t.Errorf("decoded series does not match encoded data!\nexpected:\n%v\ngot:\n%v\n", pretty(in), pretty(out))
	}
}

// rand returns a number in the range [low,hi)
// caller should assure that hi > low
func randUint32Range(low, hi uint32) uint32 {
	return low + uint32(rand.Intn(int(hi-low)))
}

func pretty(vals []schema.Point) string {
	var buf bytes.Buffer
	for i, point := range vals {
		fmt.Fprintf(&buf, "%5d  %d %f\n", i, point.Ts, point.Val)
	}
	return buf.String()
}

// copy from the chunk package because we can't circular import
// we should probably just merge the chunk and tsz packages and clean this up.
var ChunkSpans = [32]uint32{
	1,
	5,
	10,
	15,
	20,
	30,
	60,        // 1m
	90,        // 1.5m
	2 * 60,    // 2m
	3 * 60,    // 3m
	5 * 60,    // 5m
	10 * 60,   // 10m
	15 * 60,   // 15m
	20 * 60,   // 20m
	30 * 60,   // 30m
	45 * 60,   // 45m
	3600,      // 1h
	90 * 60,   // 1.5h
	2 * 3600,  // 2h
	150 * 60,  // 2.5h
	3 * 3600,  // 3h
	4 * 3600,  // 4h
	5 * 3600,  // 5h
	6 * 3600,  // 6h
	7 * 3600,  // 7h
	8 * 3600,  // 8h
	9 * 3600,  // 9h
	10 * 3600, // 10h
	12 * 3600, // 12h
	15 * 3600, // 15h
	18 * 3600, // 18h
	24 * 3600, // 24h
}
