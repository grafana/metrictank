package test

import (
	"fmt"
	"reflect"

	"github.com/grafana/metrictank/pkg/schema"
)

func GetAMKey(suffix int) schema.AMKey {
	return schema.AMKey{
		MKey: GetMKey(suffix),
	}
}

func GetMKey(suffix int) schema.MKey {
	s := uint32(suffix)
	return schema.MKey{
		Org: 1,
		Key: [16]byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, byte(s >> 24), byte(s >> 16), byte(s >> 8), byte(s)},
	}
}

func KeyToInt(key schema.MKey) int {
	return int(uint32(key.Key[15]) + uint32(key.Key[14])<<8 + uint32(key.Key[13])<<16 + uint32(key.Key[12])<<24)
}

func MustMKeyFromString(id string) schema.MKey {
	mkey, err := schema.MKeyFromString(id)
	if err != nil {
		panic(err)
	}
	return mkey
}

func ContainsMKey(list []schema.MKey, subject schema.MKey) bool {
	for _, v := range list {
		if reflect.DeepEqual(v, subject) {
			return true
		}
	}
	return false
}

func ShouldContainMKey(actual interface{}, expected ...interface{}) string {
	list := expected[0].([]schema.MKey)
	subject := actual.(schema.MKey)
	if !ContainsMKey(list, subject) {
		return fmt.Sprintf("slice of MKey's did not contain %v", subject)
	}
	return ""
}
