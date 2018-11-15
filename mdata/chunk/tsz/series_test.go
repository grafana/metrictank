package tsz

import (
	"bytes"
	"fmt"
	"reflect"
	"testing"

	"github.com/raintank/schema"
)

type testCase struct {
	desc string
	t0   uint32
	vals []schema.Point
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
			makeVals(1540728000, 1540728000+3600*6, 60, 0, -1),
		},
		{
			"regular chunk with dense, 1s spread data",
			1540728000,
			makeVals(1540728000, 1540728000+3600*6, 1, 0, -1),
		},
		{
			"regular chunk with sparse, 60s spread data",
			1540728000,
			makeVals(1540728000, 1540728000+3600*6, 60, 0, 3),
		},
		{
			"regular chunk with sparse, 1s spread data",
			1540728000,
			makeVals(1540728000, 1540728000+3600*6, 1, 0, 3),
		},
		{
			"regular chunk with sparse, 60s spread data. start with a skip",
			1540728000,
			makeVals(1540728000, 1540728000+3600*6, 60, 3, 3),
		},
		{
			"regular chunk with sparse, 1s spread data. start with a skip",
			1540728000,
			makeVals(1540728000, 1540728000+3600*6, 1, 3, 3),
		},
		{
			"a single point in the beginning",
			1540728000,
			[]schema.Point{{1540728000, 1540728000}},
		},
		{
			"a single point (the 2nd point after to)",
			1540728000,
			[]schema.Point{{1540728000 + 60, 1540728000 + 60}},
		},
		{
			"a single point right at the end",
			1540728000,
			[]schema.Point{{1540749600 - 60, 1540749600 - 60}},
		},
		{
			"a single point in the beginning + one at the end",
			1540728000,
			[]schema.Point{{1540728000, 1540728000}, {1540749600 - 60, 1540749600 - 60}},
		},
		{
			"a point (the 2nd point after to) + one at the end",
			1540728000,
			[]schema.Point{{1540728000 + 60, 1540728000 + 60}, {1540749600 - 60, 1540749600 - 60}},
		},
		{
			"no data for 5 hours, then 1 hour of 60s dense data",
			1540728000,
			makeVals(1540728000+3600*5, 1540728000+3600*6, 60, 0, -1),
		},
		{
			"no data for 5 hours, then 1 hour of 60s sparse data",
			1540728000,
			makeVals(1540728000+3600*5, 1540728000+3600*6, 60, 0, 3),
		},
		{
			"no data for 5 hours, then 1 hour of 60s sparser data",
			1540728000,
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

		iter4h, err := NewIterator4h(bytes4h)
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

func pretty(vals []schema.Point) string {
	var buf bytes.Buffer
	for i, point := range vals {
		fmt.Fprintf(&buf, "%5d  %d %f\n", i, point.Ts, point.Val)
	}
	return buf.String()
}
