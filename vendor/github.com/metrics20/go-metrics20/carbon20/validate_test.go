package carbon20

import (
	"testing"
	"math"

	"github.com/bmizerany/assert"
)

func TestValidateLegacy(t *testing.T) {
	cases := []struct {
		in    string
		level ValidationLevelLegacy
		valid bool
	}{
		{"foo.bar", StrictLegacy, true},
		{"foo.bar", MediumLegacy, true},
		{"foo.bar", NoneLegacy, true},
		{"foo..bar", StrictLegacy, false},
		{"foo..bar", MediumLegacy, true},
		{"foo..bar", NoneLegacy, true},
		{"foo..bar.ba::z", StrictLegacy, false},
		{"foo..bar.ba::z", MediumLegacy, true},
		{"foo..bar.ba::z", NoneLegacy, true},
		{"foo..bar.b\xbdz", StrictLegacy, false},
		{"foo..bar.b\xbdz", MediumLegacy, false},
		{"foo..bar.b\xbdz", NoneLegacy, true},
		{"foo..bar.b\x00z", StrictLegacy, false},
		{"foo..bar.b\x00z", MediumLegacy, false},
		{"foo..bar.b\x00z", NoneLegacy, true},
	}
	for _, c := range cases {
		assert.Equal(t, ValidateKeyLegacy(c.in, c.level) == nil, c.valid)
		assert.Equal(t, ValidateKeyLegacyB([]byte(c.in), c.level) == nil, c.valid)
	}
}

func TestValidateTimestamps(t *testing.T) {
	cases := []struct {
		in    string
		valid bool
	}{
		{"foo.bar 1 123", true},
		{"foo.bar 1 123.0", true},
		{"foo.bar 1 123abc", false},
	}

	for _, c := range cases {
		_, _, _, err := ValidatePacket([]byte(c.in), NoneLegacy, NoneM20)
		valid := err == nil
		if valid != c.valid {
			t.Errorf("in='%s' valid=%v, expected %v", c.in, valid, c.valid)
		}
	}
}

func TestValidateValues(t *testing.T) {
	cases := []struct {
		in    string
		value float64
		valid bool
	}{
		{"foo.bar -1 123", -1, true},
		{"foo.bar 1e5 123", 1e5, true},
		{"foo.bar 1E+5 123", 1e5, true},
		{"foo.bar +1E-5 123", 1e-5, true},
		{"foo.bar 1e100 123", 1e100, true},
		{"foo.bar z1 123", 0, false},
		{"foo.bar ++1 123", 0, false},
	}

	for _, c := range cases {
		_, val, _, err := ValidatePacket([]byte(c.in), NoneLegacy, NoneM20)
		valid := err == nil
		if valid != c.valid {
			t.Errorf("in='%s' valid=%v, expected %v", c.in, valid, c.valid)
		}
		if math.Abs(val - c.value) > 0.0001 {
			t.Errorf("in='%s', value=%v, expected %v", c.in, val, c.value)
		}
	}
}


func TestValidateM20(t *testing.T) {
	cases := []struct {
		in    string
		level ValidationLevelM20
		valid bool
	}{
		{"foo.bar.aunit=no.baz", MediumM20, false},
		{"foo.bar.UNIT=no.baz", MediumM20, false},
		{"foo.bar.unita=no.bar", MediumM20, false},
		{"foo.bar.aunit=no.baz", NoneM20, true},
		{"foo.bar.UNIT=no.baz", NoneM20, true},
		{"foo.bar.unita=no.bar", NoneM20, true},
	}
	for _, c := range cases {
		assert.Equal(t, ValidateKeyM20(c.in, c.level) == nil, c.valid)
		assert.Equal(t, ValidateKeyM20B([]byte(c.in), c.level) == nil, c.valid)
	}
}
func TestValidateM20NoEquals(t *testing.T) {
	cases := []struct {
		in    string
		level ValidationLevelM20
		valid bool
	}{
		{"foo.bar.mtype_is_count.baz", MediumM20, false},
		{"foo.bar.mtype_is_count", MediumM20, false},
		{"mtype_is_count.foo.bar", MediumM20, false},
		{"foo.bar.mtype_is_count.baz", NoneM20, true},
		{"foo.bar.mtype_is_count", NoneM20, true},
		{"mtype_is_count.foo.bar", NoneM20, true},
	}
	for _, c := range cases {
		assert.Equal(t, ValidateKeyM20NoEquals(c.in, c.level) == nil, c.valid)
		assert.Equal(t, ValidateKeyM20NoEqualsB([]byte(c.in), c.level) == nil, c.valid)
	}
}

func BenchmarkValidatePacketNone(b *testing.B) {
	in := []byte("carbon.agents.foo.cache.overflow 123.456 1234567890")
	for i := 0; i < b.N; i++ {
		_, _, _, err := ValidatePacket(in, NoneLegacy, NoneM20)
		if err != nil {
			panic(err)
		}
	}
}
func BenchmarkValidatePacketMedium(b *testing.B) {
	in := []byte("carbon.agents.foo.cache.overflow 123.456 1234567890")
	for i := 0; i < b.N; i++ {
		_, _, _, err := ValidatePacket(in, MediumLegacy, NoneM20)
		if err != nil {
			panic(err)
		}
	}
}
func BenchmarkValidatePacketStrict(b *testing.B) {
	in := []byte("carbon.agents.foo.cache.overflow 123.456 1234567890")
	for i := 0; i < b.N; i++ {
		_, _, _, err := ValidatePacket(in, StrictLegacy, NoneM20)
		if err != nil {
			panic(err)
		}
	}
}
