package models

import (
	"encoding/json"
	"math/rand"
	"reflect"
	"testing"

	"github.com/grafana/metrictank/schema"
)

func TestJsonMarshal(t *testing.T) {
	cases := []struct {
		in  []Series
		out string
	}{
		{
			in:  []Series{},
			out: `[]`,
		},
		{
			in: []Series{
				{
					Target:     "a",
					Datapoints: []schema.Point{},
					Interval:   60,
				},
			},
			out: `[{"target":"a","datapoints":[]}]`,
		},
		{
			in: []Series{
				{
					Target:     `a\b`,
					Datapoints: []schema.Point{},
					Interval:   60,
				},
			},
			out: `[{"target":"a\\b","datapoints":[]}]`,
		},
		{
			in: []Series{
				{
					Target: "a",
					Datapoints: []schema.Point{
						{Val: 123, Ts: 60},
						{Val: 10000, Ts: 120},
						{Val: 0, Ts: 180},
						{Val: 1, Ts: 240},
					},
					Interval: 60,
				},
			},
			out: `[{"target":"a","datapoints":[[123,60],[10000,120],[0,180],[1,240]]}]`,
		},
		{
			in: []Series{
				{
					Target: "a",
					Datapoints: []schema.Point{
						{Val: 123, Ts: 60},
						{Val: 10000, Ts: 120},
						{Val: 0, Ts: 180},
						{Val: 1, Ts: 240},
					},
					Interval: 60,
				},
				{
					Target: "foo(bar)",
					Datapoints: []schema.Point{
						{Val: 123.456, Ts: 10},
						{Val: 123.7, Ts: 20},
						{Val: 124.1001, Ts: 30},
						{Val: 125.0, Ts: 40},
						{Val: 126.0, Ts: 50},
					},
					Interval: 10,
				},
			},
			out: `[{"target":"a","datapoints":[[123,60],[10000,120],[0,180],[1,240]]},{"target":"foo(bar)","datapoints":[[123.456,10],[123.7,20],[124.1001,30],[125,40],[126,50]]}]`,
		},
	}

	for _, c := range cases {
		buf, err := json.Marshal(SeriesByTarget(c.in))
		if err != nil {
			t.Fatalf("failed to marshal to JSON. %s", err)
		}
		got := string(buf)
		if c.out != got {
			t.Fatalf("bad json output.\nexpected:%s\ngot:     %s\n", c.out, got)
		}
	}
}

func TestSetTags(t *testing.T) {
	cases := []struct {
		in  Series
		out map[string]string
	}{
		{
			in: Series{},
			out: map[string]string{
				"name": "",
			},
		},
		{
			in: Series{
				Target: "a",
			},
			out: map[string]string{
				"name": "a",
			},
		},
		{
			in: Series{
				Target: `a\b`,
			},
			out: map[string]string{
				"name": `a\b`,
			},
		},
		{
			in: Series{
				Target: "a;b=c;c=d",
			},
			out: map[string]string{
				"name": "a",
				"b":    "c",
				"c":    "d",
			},
		},
		{
			in: Series{
				Target: "a;biglongtagkeyhere=andithasabiglongtagvaluetoo;c=d",
			},
			out: map[string]string{
				"name":              "a",
				"biglongtagkeyhere": "andithasabiglongtagvaluetoo",
				"c":                 "d",
			},
		},
	}

	for _, c := range cases {
		c.in.SetTags()
		if !reflect.DeepEqual(c.out, c.in.Tags) {
			t.Fatalf("SetTags incorrect\nexpected:%v\ngot:     %v\n", c.out, c.in.Tags)
		}
	}
}

func BenchmarkSetTags_00tags_00chars(b *testing.B) {
	benchmarkSetTags(b, 0, 0, 0, true)
}

func BenchmarkSetTags_20tags_32chars(b *testing.B) {
	benchmarkSetTags(b, 20, 32, 32, true)
}

func BenchmarkSetTags_20tags_32chars_reused(b *testing.B) {
	benchmarkSetTags(b, 20, 32, 32, false)
}

func benchmarkSetTags(b *testing.B, numTags, tagKeyLength, tagValueLength int, resetTags bool) {
	in := Series{
		Target: "my.metric.name",
	}

	for i := 0; i < numTags; i++ {
		in.Target += ";" + randString(tagKeyLength) + "=" + randString(tagValueLength)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		in.SetTags()
		if len(in.Tags) != numTags+1 {
			b.Fatalf("Expected %d tags, got %d, target = %s, tags = %v", numTags+1, len(in.Tags), in.Target, in.Tags)
		}
		if resetTags {
			// Reset so as to not game the allocations
			in.Tags = nil
		}
	}
	b.SetBytes(int64(len(in.Target)))
}

func randString(n int) string {
	const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Int63()%int64(len(letterBytes))]
	}
	return string(b)
}
