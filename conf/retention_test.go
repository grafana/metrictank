package conf

import (
	"math"
	"reflect"
	"testing"
)

func TestParseRetentions(t *testing.T) {
	cases := []struct {
		in             string
		acceptableOrig string
		err            bool
		out            []Retention
	}{
		{
			in:             "1s:1d:1h:2,1m:8d:4h:2:1234567890,10m:120d:6h:1:true,30m:2y:6h:1:false",
			acceptableOrig: "1s:1d:1h:2:true,1m:1w1d:4h:2:1234567890,10m:17w1d:6h:1:true,30m:2y:6h:1:false", // equivalent to 'in'
			err:            false,
			out: []Retention{
				{
					SecondsPerPoint: 1,
					NumberOfPoints:  24 * 3600,
					ChunkSpan:       60 * 60,
					NumChunks:       2,
					Ready:           0,
				},
				{
					SecondsPerPoint: 60,
					NumberOfPoints:  8 * 24 * 3600 / 60,
					ChunkSpan:       4 * 60 * 60,
					NumChunks:       2,
					Ready:           1234567890,
				},
				{
					SecondsPerPoint: 600,
					NumberOfPoints:  120 * 24 * 3600 / 600,
					ChunkSpan:       6 * 60 * 60,
					NumChunks:       1,
					Ready:           0,
				},
				{
					SecondsPerPoint: 30 * 60,
					NumberOfPoints:  2 * 365 * 24 * 3600 / (30 * 60),
					ChunkSpan:       6 * 60 * 60,
					NumChunks:       1,
					Ready:           math.MaxUint32,
				},
			},
		},
	}
	for i, c := range cases {
		got, err := ParseRetentions(c.in)
		if (err != nil) != c.err {
			t.Fatalf("case %d: exp error %t but got err %v", i, c.err, err)
		}
		if c.err {
			continue
		}
		exp := Retentions{
			Orig: c.in,
			Rets: c.out,
		}
		if !reflect.DeepEqual(Retentions(exp), got) {
			t.Fatalf("case %d: exp retentions\n%v\nbut got\n%v", i, exp, got)
		}
		orig := buildOrigFromRetentions(got.Rets)
		if orig != c.acceptableOrig {
			t.Fatalf("case %d: exp orig\n%v\nbut got\n%v", i, c.in, orig)
		}

	}
}
