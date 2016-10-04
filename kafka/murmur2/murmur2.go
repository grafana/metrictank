/*
Go implemtation of the murmur2 hashing used by kafka
https://apache.googlesource.com/kafka/+/0.10.0.1/clients/src/main/java/org/apache/kafka/common/utils/Utils.java#340

*/
package murmur2

// Mixing constants; generated offline.
const (
	M    = 0x5bd1e995
	R    = 24
	seed = -1756908916
)

// -----------------------------------------------------------------------------
func MurmurHash2(data []byte) (h int32) {
	var k int32
	// Initialize the hash to a 'random' value
	h = int32(seed) &^ int32(len(data))
	length4 := len(data) / 4

	for i := 0; i < length4; i++ {
		i4 := i * 4
		k = (int32(data[i4+0] & 0xff)) + (int32(data[i4+1]&0xff) << 8) + (int32(data[i4+2]&0xff) << 16) + (int32(data[i4+3]&0xff) << 24)
		k *= M
		k ^= int32(uint32(k) >> R)
		k *= M
		h *= M
		h ^= k
	}
	// Handle the last few bytes of the input array
	switch len(data) % 4 {
	case 3:
		h ^= int32(data[(len(data) & ^3)+2]&0xff) << 16
		fallthrough
	case 2:
		h ^= int32(data[(len(data) & ^3)+1]&0xff) << 8
		fallthrough
	case 1:
		h ^= int32(data[(len(data) & ^3)] & 0xff)
		h *= M
	}
	h ^= int32(uint32(h) >> 13)
	h *= M
	h ^= int32(uint32(h) >> 15)
	return
}
