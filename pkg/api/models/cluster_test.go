package models

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/grafana/metrictank/internal/schema"
)

// TestGetDataRespV0V1Compat tests that GetDataRespV0 and GetDataRespV1 can be used interchangeably
// e.g. in a cluster being in-place upgraded.
func TestGetDataRespV0V1Compat(t *testing.T) {
	series := []Series{
		{
			Target:     "an.empty.series",
			Datapoints: []schema.Point{{Val: 5, Ts: 123}},
			Tags:       map[string]string{"foo": "bar"},
			Interval:   10,
		},
	}

	// test that a marshaled v0 can be unmarshaled as a v1 with empty stats

	v0 := GetDataRespV0{series}
	buf, err := v0.MarshalMsg(nil)
	if err != nil {
		t.Fatalf("failed to v0.MarshalMsg: %s", err.Error())
	}

	var v1 GetDataRespV1
	_, err = v1.UnmarshalMsg(buf)
	if err != nil {
		t.Fatalf("failed to v1.UnmarshalMsg: %s", err.Error())
	}
	var stats StorageStats
	if diff := cmp.Diff(series, v1.Series); diff != "" {
		t.Errorf("series mismatch (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(stats, v1.Stats); diff != "" {
		t.Errorf("series mismatch (-want +got):\n%s", diff)
	}

	// test that a marshaled v1 can be unmarshaled as a v0 without the stats

	v1 = GetDataRespV1{
		Series: series,
	}

	buf, err = v1.MarshalMsg(nil)
	if err != nil {
		t.Fatalf("failed to v1.MarshalMsg: %s", err.Error())
	}
	_, err = v0.UnmarshalMsg(buf)
	if err != nil {
		t.Fatalf("failed to v0.UnmarshalMsg: %s", err.Error())
	}
	if diff := cmp.Diff(v1.Series, v0.Series); diff != "" {
		t.Errorf("series mismatch (-want +got):\n%s", diff)
	}
}
