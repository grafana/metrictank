package main

import (
	"testing"
	"time"

	"github.com/grafana/metrictank/cluster"
	"github.com/grafana/metrictank/mdata/chunk"
	"github.com/grafana/metrictank/schema"
)

func getChunk(interval int, t0, span uint32) []byte {
	c := chunk.New(t0)
	for i := t0; i < t0+span; i += uint32(interval) {
		c.Push(i, float64(i))
	}
	c.Finish()
	return c.Encode(span)
}

const sixHour = 60 * 60 * 6

var md *schema.MetricDefinition

func init() {
	md = &schema.MetricDefinition{
		Name:     "foo.bar.baz",
		Interval: 60,
		Mtype:    "gauge",
	}
	md.SetId()

	cluster.Init("mt-cassandra-importer", "0.1", time.Now(), "http", 6060)
	cluster.Manager.SetPrimary(true)

}

func Test_aggregators_aggregate(t *testing.T) {
	store := &AggStore{}
	a := newAggregators(md, store, sixHour, sixHour*4)
	type args struct {
		data []byte
		ts   int
	}
	tests := []struct {
		name        string
		aggregators *aggregators
		args        args
		wantErr     bool
		validate    func(t *testing.T, a *aggregators)
	}{
		{
			// processing the first chunk will start the rollup, but it wont be until the first point of the second chunk
			// is handled, that the first rollup points will be created
			name:        "test first chunk",
			aggregators: a,
			args: args{
				ts:   sixHour,
				data: getChunk(60, sixHour, uint32(sixHour)),
			},
			wantErr: false,
			validate: func(t *testing.T, a *aggregators) {
				cwrs := a.store.Get()
				if len(cwrs) != 0 {
					t.Errorf("aggregators.aggregate() expected %d chunkWriteRequests, got %d", 0, len(cwrs))
				}
				return
			},
		},
		{
			// processing the second chunk will cause new rollup points to be created.  but it wont be until the 3rd chunk
			// is processed that a chunk will be complete
			name:        "test second chunk",
			aggregators: a,
			args: args{
				ts:   sixHour * 2,
				data: getChunk(60, sixHour*2, uint32(sixHour)),
			},
			wantErr: false,
			validate: func(t *testing.T, a *aggregators) {
				cwrs := a.store.Get()
				if len(cwrs) != 0 {
					t.Errorf("aggregators.aggregate() expected %d chunkWriteRequests, got %d", 0, len(cwrs))
				}
				return
			},
		},
		{
			// the first point in the 3rd chunk will complete the last rollup point of the previous chunk.
			// When the second rollup points are processed, it will cause the first rollup chunk to be finished.
			name:        "test third chunk",
			aggregators: a,
			args: args{
				ts:   sixHour * 3,
				data: getChunk(60, sixHour*3, uint32(sixHour)),
			},
			wantErr: false,
			validate: func(t *testing.T, a *aggregators) {
				cwrs := a.store.Get()
				// 4 chunks should have been created, sum and cnt for 300s and 1200s
				if len(cwrs) != 4 {
					t.Errorf("aggregators.aggregate() expected %d chunkWriteRequests, got %d", 4, len(cwrs))
				}

				return
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			if err := tt.aggregators.aggregate(tt.args.data, tt.args.ts); (err != nil) != tt.wantErr {
				t.Errorf("aggregators.aggregate() error = %v, wantErr %v", err, tt.wantErr)
			}
			tt.validate(t, tt.aggregators)
		})
	}
}
