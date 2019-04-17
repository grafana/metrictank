package chunk

import (
	"encoding/hex"
	"math"
	"testing"

	"github.com/grafana/metrictank/mdata/errors"
	"github.com/raintank/schema"
)

type prodChunk struct {
	t0   uint32
	data string
	exp  []schema.Point
}

// these tests are real 4h chunks we pulled out of production that have the overflow bug
// they only contain 1 point so remediation is less obvious
func TestRealProduction4hChunksWithSinglePointThatNeedRemediation(t *testing.T) {
	realChunks := []prodChunk{
		{
			// Nov7
			t0:   1541332800,
			data: "01175bdedf4035610396070000000003ffffffffc0",
			exp: []schema.Point{
				{44046, 1541352600},
			},
		},
		{
			// Nov15
			t0:   1541851200,
			data: "01175be6c84035610370880000000003ffffffffc0",
			exp: []schema.Point{
				{28808, 1541871000},
			},
		},
	}
	for i, rc := range realChunks {
		data, err := hex.DecodeString(rc.data)
		if err != nil {
			t.Fatal(err)
		}
		itgen, err := NewIterGen(rc.t0, 30*60, data)
		if err != nil {
			t.Errorf("case %d: could not construct itergen: %s", i, err)
		}
		iter, err := itgen.Get()
		if err != nil {
			t.Errorf("case %d: could not get iterator: %s", i, err)
		}
		var got []schema.Point
		for iter.Next() {
			ts, val := iter.Values()
			got = append(got, schema.Point{val, ts})
		}
		if !equal(rc.exp, got) {
			t.Errorf("case %d: output mismatch:\nexpected:\n%v\ngot:\n%v", i, rc.exp, got)
		}
		err = iter.Err()
		if err != nil {
			t.Errorf("case %d: iter.Err returned %v", i, err)
		}
	}
}

func equal(exp, got []schema.Point) bool {
	if len(exp) != len(got) {
		return false
	}
	for i, pgot := range got {
		pexp := exp[i]
		if math.IsNaN(pgot.Val) != math.IsNaN(pexp.Val) {
			return false
		}
		if !math.IsNaN(pgot.Val) && pgot.Val != pexp.Val {
			return false
		}
		if pgot.Ts != pexp.Ts {
			return false
		}
	}
	return true
}

type expectedData struct {
	NumPoints uint32
	Err       error
}

func testPush(t *testing.T, points []schema.Point, expected []expectedData) {
	chunk := New(0)
	for i := range points {
		err := chunk.Push(points[i].Ts, points[i].Val)
		if chunk.NumPoints != expected[i].NumPoints {
			t.Fatalf("Expected %d points pushed but had %d", expected[i].NumPoints, chunk.NumPoints)
		}
		if err != expected[i].Err {
			t.Fatalf("Expected error %v but received %v", expected[i].Err, err)
		}
	}
}

func TestChunkPush(t *testing.T) {
	points := []schema.Point{
		{Ts: 1001, Val: 100},
		{Ts: 1002, Val: 100},
		{Ts: 1002, Val: 100},
		{Ts: 1003, Val: 100},
		{Ts: 999, Val: 100},
	}
	expected := []expectedData{
		{1, nil},
		{2, nil},
		{2, errors.ErrMetricNewValueForTimestamp},
		{3, nil},
		{3, errors.ErrMetricTooOld},
	}

	testPush(t, points, expected)
}
