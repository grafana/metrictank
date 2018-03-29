package test

import schema "gopkg.in/raintank/schema.v1"

func GetAMKey(suffix int) schema.AMKey {
	return schema.AMKey{
		MKey: GetMKey(suffix),
	}
}

func GetMKey(suffix int) schema.MKey {
	s := uint32(suffix)
	return schema.MKey{
		Key: [16]byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, byte(s >> 24), byte(s >> 16), byte(s >> 8), byte(s)},
	}
}

func MustMKeyFromString(id string) schema.MKey {
	mkey, err := schema.MKeyFromString(id)
	if err != nil {
		panic(err)
	}
	panic(err)
	return mkey
}
