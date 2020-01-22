package conf

import (
	"math"
	"reflect"
	"regexp"
	"testing"

	"github.com/google/go-cmp/cmp"
	. "github.com/smartystreets/goconvey/convey"
)

func schemasForTest() Schemas {
	return NewSchemas([]Schema{
		{
			Name:    "a",
			Pattern: regexp.MustCompile("^a\\..*"),
			Retentions: BuildFromRetentions(
				NewRetentionMT(10, 3600, 60*10, 0, 0),
				NewRetentionMT(3600, 86400, 60*60*6, 0, 0),
			),
		},
		{
			Name:    "b",
			Pattern: regexp.MustCompile("^b\\..*"),
			Retentions: BuildFromRetentions(
				NewRetentionMT(1, 60, 60*10, 0, 0),
				NewRetentionMT(30, 120, 60*30, 0, 0),
				NewRetentionMT(600, 86400, 60*60*6, 0, 0),
			),
		},
		{
			Name:    "default",
			Pattern: regexp.MustCompile(".*"),
			Retentions: BuildFromRetentions(
				NewRetentionMT(1, 60, 60*10, 0, 0),
				NewRetentionMT(60, 3600, 60*60*2, 0, 0),
				NewRetentionMT(600, 86400, 60*60*6, 0, 0),
				NewRetentionMT(3600, 86400*7, 60*60*6, 0, 0),
			),
		},
	})
}

func TestMatch(t *testing.T) {
	schemas := schemasForTest()
	Convey("When matching against first schema", t, func() {
		Convey("When metric has 1s raw interval", func() {
			id, schema := schemas.Match("a.foo", 1)
			So(id, ShouldEqual, 0)
			So(schema.Name, ShouldEqual, "a")
			So(schema.Retentions.Rets[0].SecondsPerPoint, ShouldEqual, 10)
		})
		Convey("When metric has 10s raw interval", func() {
			id, schema := schemas.Match("a.foo", 10)
			So(id, ShouldEqual, 0)
			So(schema.Name, ShouldEqual, "a")
			So(schema.Retentions.Rets[0].SecondsPerPoint, ShouldEqual, 10)
		})
		Convey("When metric has 30s raw interval", func() {
			id, schema := schemas.Match("a.foo", 30)
			So(id, ShouldEqual, 0)
			So(schema.Name, ShouldEqual, "a")
			So(schema.Retentions.Rets[0].SecondsPerPoint, ShouldEqual, 10)
		})
		Convey("When metric has 2h raw interval", func() {
			id, schema := schemas.Match("a.foo", 7200)
			So(id, ShouldEqual, 1)
			So(schema.Name, ShouldEqual, "a")
			So(schema.Retentions.Rets[0].SecondsPerPoint, ShouldEqual, 3600)
		})
	})
	Convey("When matching against second schema", t, func() {
		Convey("When metric has 1s raw interval", func() {
			id, schema := schemas.Match("b.foo", 1)
			So(id, ShouldEqual, 2)
			So(schema.Name, ShouldEqual, "b")
			So(schema.Retentions.Rets[0].SecondsPerPoint, ShouldEqual, 1)
		})
		Convey("When metric has 10s raw interval", func() {
			id, schema := schemas.Match("b.foo", 10)
			So(id, ShouldEqual, 2)
			So(schema.Name, ShouldEqual, "b")
			So(schema.Retentions.Rets[0].SecondsPerPoint, ShouldEqual, 1)
		})
		Convey("When metric has 30s raw interval", func() {
			id, schema := schemas.Match("b.foo", 30)
			So(id, ShouldEqual, 3)
			So(schema.Name, ShouldEqual, "b")
			So(schema.Retentions.Rets[0].SecondsPerPoint, ShouldEqual, 30)
		})
		Convey("When metric has 2h raw interval", func() {
			id, schema := schemas.Match("b.foo", 7200)
			So(id, ShouldEqual, 4)
			So(schema.Name, ShouldEqual, "b")
			So(schema.Retentions.Rets[0].SecondsPerPoint, ShouldEqual, 600)
		})
	})
	Convey("When matching against default schema", t, func() {
		Convey("When metric has 1s raw interval", func() {
			id, schema := schemas.Match("c.foo", 1)
			So(id, ShouldEqual, 5)
			So(schema.Name, ShouldEqual, "default")
			So(schema.Retentions.Rets[0].SecondsPerPoint, ShouldEqual, 1)
		})
		Convey("When metric has 10s raw interval", func() {
			id, schema := schemas.Match("c.foo", 10)
			So(id, ShouldEqual, 5)
			So(schema.Name, ShouldEqual, "default")
			So(schema.Retentions.Rets[0].SecondsPerPoint, ShouldEqual, 1)
		})
		Convey("When metric has 30s raw interval", func() {
			id, schema := schemas.Match("c.foo", 60)
			So(id, ShouldEqual, 6)
			So(schema.Name, ShouldEqual, "default")
			So(schema.Retentions.Rets[0].SecondsPerPoint, ShouldEqual, 60)
		})
		Convey("When metric has 2h raw interval", func() {
			id, schema := schemas.Match("c.foo", 7200)
			So(id, ShouldEqual, 8)
			So(schema.Name, ShouldEqual, "default")
			So(schema.Retentions.Rets[0].SecondsPerPoint, ShouldEqual, 3600)
		})
	})
}

func TestDefaultSchema(t *testing.T) {
	schemas := NewSchemas([]Schema{
		{
			Name:    "a",
			Pattern: regexp.MustCompile("^a\\..*"),
			Retentions: BuildFromRetentions(
				NewRetentionMT(10, 3600, 60*10, 0, 0),
				NewRetentionMT(3600, 86400, 60*60*6, 0, 0),
			),
		},
	})

	Convey("When matching against first schema", t, func() {
		Convey("When metric has 1s raw interval", func() {
			id, schema := schemas.Match("a.foo", 1)
			So(id, ShouldEqual, 0)
			So(schema.Name, ShouldEqual, "a")
			So(schema.Retentions.Rets[0].SecondsPerPoint, ShouldEqual, 10)
		})
	})
	Convey("When series doesnt match any schema", t, func() {
		Convey("When metric has 10s raw interval", func() {
			id, schema := schemas.Match("d.foo", 10)
			So(id, ShouldEqual, 2)
			So(schema.Name, ShouldEqual, "default")
			So(schema.Retentions.Rets[0].SecondsPerPoint, ShouldEqual, 1)
		})
	})
}

func TestTTLs(t *testing.T) {
	schemas := schemasForTest()
	Convey("When getting list of TTLS", t, func() {
		ttls := schemas.TTLs()
		So(len(ttls), ShouldEqual, 5)
	})
}
func TestMaxChunkSpan(t *testing.T) {
	schemas := schemasForTest()
	Convey("When getting maxChunkSpan", t, func() {
		max := schemas.MaxChunkSpan()
		So(max, ShouldEqual, 60*60*6)
	})
}

func TestReadSchemas(t *testing.T) {
	tests := []struct {
		name    string
		file    string
		want    Schemas
		wantErr bool
	}{
		{
			name:    "empty_file",
			file:    "schemas_test_files/empty.schemas",
			want:    NewSchemas(nil),
			wantErr: false,
		},
		{
			name:    "no_pattern",
			file:    "schemas_test_files/no_pattern.schemas",
			want:    Schemas{},
			wantErr: true,
		},
		{
			name:    "bad_pattern",
			file:    "schemas_test_files/bad_pattern.schemas",
			want:    Schemas{},
			wantErr: true,
		},
		{
			name:    "bad_retention",
			file:    "schemas_test_files/bad_retention.schemas",
			want:    Schemas{},
			wantErr: true,
		},
		{
			name: "simple",
			file: "schemas_test_files/simple.schemas",
			want: NewSchemas([]Schema{
				{
					Name:    "default",
					Pattern: regexp.MustCompile(".*"),
					Retentions: Retentions{
						Orig: "1s:8d:10min:2,1m:35d:2h:2,10m:120d:6h:2,1h:2y:6h:2",
						Rets: []Retention{
							NewRetentionMT(1, 8*24*60*60, 10*60, 2, 0),
							NewRetentionMT(1*60, 35*24*60*60, 2*60*60, 2, 0),
							NewRetentionMT(10*60, 120*24*60*60, 6*60*60, 2, 0),
							NewRetentionMT(1*60*60, 2*365*24*60*60, 6*60*60, 2, 0),
						},
					},
					Priority: -1,
				},
			}),
			wantErr: false,
		},
		{
			name: "reorder_buffer",
			file: "schemas_test_files/reorder_buffer.schemas",
			want: NewSchemas([]Schema{
				{
					Name:    "default",
					Pattern: regexp.MustCompile(".*"),
					Retentions: Retentions{
						Orig: "1s:8d:10min:2,1m:35d:2h:2,10m:120d:6h:2,1h:2y:6h:2",
						Rets: []Retention{
							NewRetentionMT(1, 8*24*60*60, 10*60, 2, 0),
							NewRetentionMT(1*60, 35*24*60*60, 2*60*60, 2, 0),
							NewRetentionMT(10*60, 120*24*60*60, 6*60*60, 2, 0),
							NewRetentionMT(1*60*60, 2*365*24*60*60, 6*60*60, 2, 0),
						},
					},
					Priority:      -1,
					ReorderWindow: 20,
				},
			}),
			wantErr: false,
		},
		{
			name: "reorder_buffer_allow_update",
			file: "schemas_test_files/reorder_buffer_allow_update.schemas",
			want: NewSchemas([]Schema{
				{
					Name:    "default",
					Pattern: regexp.MustCompile(".*"),
					Retentions: Retentions{
						Orig: "1s:8d:10min:2,1m:35d:2h:2,10m:120d:6h:2,1h:2y:6h:2",
						Rets: []Retention{
							NewRetentionMT(1, 8*24*60*60, 10*60, 2, 0),
							NewRetentionMT(1*60, 35*24*60*60, 2*60*60, 2, 0),
							NewRetentionMT(10*60, 120*24*60*60, 6*60*60, 2, 0),
							NewRetentionMT(1*60*60, 2*365*24*60*60, 6*60*60, 2, 0),
						},
					},
					Priority:           -1,
					ReorderWindow:      20,
					ReorderAllowUpdate: true,
				},
			}),
			wantErr: false,
		},
		{
			name: "multiple",
			file: "schemas_test_files/multiple.schemas",
			want: NewSchemas([]Schema{
				{
					Name:    "raw",
					Pattern: regexp.MustCompile("fakemetrics.raw"),
					Retentions: Retentions{
						Orig: "1s:6h:2min:2",
						Rets: []Retention{
							NewRetentionMT(1, 6*60*60, 2*60, 2, 0),
						},
					},
					Priority: -1,
				},
				{
					Name:    "default",
					Pattern: regexp.MustCompile(".*"),
					Retentions: Retentions{
						Orig: "1s:8d:10min:2,1m:35d:2h:2,10m:120d:6h:2,1h:2y:6h:2",
						Rets: []Retention{
							NewRetentionMT(1, 8*24*60*60, 10*60, 2, 0),
							NewRetentionMT(1*60, 35*24*60*60, 2*60*60, 2, 0),
							NewRetentionMT(10*60, 120*24*60*60, 6*60*60, 2, 0),
							NewRetentionMT(1*60*60, 2*365*24*60*60, 6*60*60, 2, 0),
						},
					},
					Priority: -2,
				},
				{
					Name:    "wpUsageMetrics",
					Pattern: regexp.MustCompile("^wp-usage"),
					Retentions: Retentions{
						Orig: "1h:35d:6h:2,2h:2y:6h:2",
						Rets: []Retention{
							NewRetentionMT(1*60*60, 35*24*60*60, 6*60*60, 2, 0),
							NewRetentionMT(2*60*60, 2*365*24*60*60, 6*60*60, 2, 0),
						},
					},
					Priority: -3,
				},
			}),
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ReadSchemas(tt.file)
			if (err != nil) != tt.wantErr {
				t.Errorf("ReadSchemas() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ReadSchemas() =\n %v\n want\n %v", got, tt.want)
			}
		})
	}
}
func TestSub(t *testing.T) {
	in := Retentions{
		Orig: "10s:600s:60s:2:true,30s:1h:60s:2:false",
		Rets: []Retention{
			{
				SecondsPerPoint: 10,
				NumberOfPoints:  60,
				ChunkSpan:       60,
				NumChunks:       2,
				Ready:           0,
			},
			{
				SecondsPerPoint: 30,
				NumberOfPoints:  120,
				ChunkSpan:       60,
				NumChunks:       2,
				Ready:           uint32(math.MaxUint32),
			},
		},
	}
	want := Retentions{
		Orig: "30s:1h:60s:2:false",
		Rets: []Retention{
			{
				SecondsPerPoint: 30,
				NumberOfPoints:  120,
				ChunkSpan:       60,
				NumChunks:       2,
				Ready:           uint32(math.MaxUint32),
			},
		},
	}
	got := in.Sub(1)
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("TestSub() mismatch (-want +got):\n%s", diff)
	}
}
