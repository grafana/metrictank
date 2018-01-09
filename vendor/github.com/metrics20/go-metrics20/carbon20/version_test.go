package carbon20

import (
	"testing"

	"github.com/bmizerany/assert"
)

var version metricVersion

func TestValidate(t *testing.T) {
	cases := []struct {
		in      string
		version metricVersion
	}{
		{"foo.bar", Legacy},
		{"foo.bar", Legacy},
		{"foo.bar", Legacy},
		{"foo..bar", Legacy},
		{"foo..bar", Legacy},
		{"foo..bar", Legacy},
		{"foo..bar.ba::z", Legacy},
		{"foo..bar.ba::z", Legacy},
		{"foo..bar.ba::z", Legacy},
		{"foo..bar.b\xbdz", Legacy},
		{"foo..bar.b\xbdz", Legacy},
		{"foo..bar.b\xbdz", Legacy},
		{"foo..bar.b\x00z", Legacy},
		{"foo..bar.b\x00z", Legacy},
		{"foo..bar.b\x00z", Legacy},
		{"foo.bar.aunit=no.baz", M20},
		{"foo.bar.UNIT=no.baz", M20},
		{"foo.bar.unita=no.bar", M20},
		{"foo.bar.mtype_is_count.baz", M20NoEquals},
		{"foo.bar.mtype_is_count", M20NoEquals},
		{"mtype_is_count.foo.bar", M20NoEquals},
	}
	for _, c := range cases {
		version := GetVersion(c.in)
		assert.Equal(t, c.version, version)
	}
}

func TestGetVersionB(t *testing.T) {
	cases := []struct {
		in []byte
		v  metricVersion
	}{
		{
			[]byte("service=carbon.instance=foo.unit=Err.mtype=gauge.type=cache_overflow"),
			M20,
		},
		{
			[]byte("service_is_carbon.instance_is_foo.unit_is_Err.mtype_is_gauge.type_is_cache_overflow"),
			M20NoEquals,
		},
		{
			[]byte("carbon.agents.foo.cache.overflow"),
			Legacy,
		},
		{
			[]byte("foo-bar"),
			Legacy,
		},
	}
	for i, c := range cases {
		v := GetVersionB(c.in)
		if v != c.v {
			t.Fatalf("case %d: expected %s, got %s", i, c.v, v)
		}
	}
}

func BenchmarkGetVersionBM20(b *testing.B) {
	in := []byte("service=carbon.instance=foo.unit=Err.mtype=gauge.type=cache_overflow")
	var v metricVersion
	for i := 0; i < b.N; i++ {
		v = GetVersionB(in)
	}
	version = v
}

func BenchmarkGetVersionBM20NoEquals(b *testing.B) {
	in := []byte("service_is_carbon.instance_is_foo.unit_is_Err.mtype_is_gauge.type_is_cache_overflow")
	var v metricVersion
	for i := 0; i < b.N; i++ {
		v = GetVersionB(in)
	}
	version = v
}

func BenchmarkGetVersionBLegacy(b *testing.B) {
	in := []byte("carbon.agents.foo.cache.overflow")
	var v metricVersion
	for i := 0; i < b.N; i++ {
		v = GetVersionB(in)
	}
	version = v
}
