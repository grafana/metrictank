package conf

import (
	"math"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestParseRetentions(t *testing.T) {
	cases := []struct {
		in  string
		err bool
		out Retentions
	}{
		{
			in:  "1s:1d:1h:2,1m:8d:4h:2:1234567890,10m:120d:6h:1:true,30m:2y:6h:1:false",
			err: false,
			out: []Retention{
				{
					SecondsPerPoint: 1,
					NumberOfPoints:  24 * 3600,
					ChunkSpan:       60 * 60,
					NumChunks:       2,
					Ready:           0,
					Str:             "1s:1d:1h:2",
				},
				{
					SecondsPerPoint: 60,
					NumberOfPoints:  8 * 24 * 3600 / 60,
					ChunkSpan:       4 * 60 * 60,
					NumChunks:       2,
					Ready:           1234567890,
					Str:             "1m:8d:4h:2:1234567890",
				},
				{
					SecondsPerPoint: 600,
					NumberOfPoints:  120 * 24 * 3600 / 600,
					ChunkSpan:       6 * 60 * 60,
					NumChunks:       1,
					Ready:           0,
					Str:             "10m:120d:6h:1:true",
				},
				{
					SecondsPerPoint: 30 * 60,
					NumberOfPoints:  2 * 365 * 24 * 3600 / (30 * 60),
					ChunkSpan:       6 * 60 * 60,
					NumChunks:       1,
					Ready:           math.MaxUint32,
					Str:             "30m:2y:6h:1:false",
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
		if diff := cmp.Diff(c.out, got); diff != "" {
			t.Fatalf("case %d: exp retentions mismatch (-want +got):\n%s", i, diff)
		}
	}
}
