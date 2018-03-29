package test

import schema "gopkg.in/raintank/schema.v1"

func GetAMKey(suffix uint8) schema.AMKey {
	return schema.AMKey{
		MKey: GetMKey(suffix),
	}
}

func GetMKey(suffix uint8) schema.MKey {
	return schema.MKey{
		Key: [16]byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, suffix},
	}
}
