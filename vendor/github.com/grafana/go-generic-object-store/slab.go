package gos

import (
	"fmt"
	"math"
	"reflect"
	"strings"
	"syscall"
	"unsafe"

	"github.com/willf/bitset"
)

// offsetOfBitSetData is the offset of the data property within the BitSet struct
var offsetOfBitSetData = reflect.TypeOf(bitset.BitSet{}).Field(1).Offset

// sizeOfBitSet is the size of the BitSet struct excluding the data that's used
// by its internal byte slice
const sizeOfBitSet = unsafe.Sizeof(bitset.BitSet{})

// slabs are actually much bigger than the slab struct. We only use it
// to look at the first byte of each slab as uint8, because that's where
// objSize is stored
type slab struct {
	objSize uint8
}

// String creates a long multi-line string which illustrates the slab in a pretty
// and human-readable format
func (s *slab) String() string {
	var b strings.Builder
	bitSet := s.bitSet()
	bitSetBytes := bitSet.Bytes()
	bitSetLen := bitSet.Len()
	objSize := uint(s.objSize)

	fmt.Fprintf(&b, "-------------------------------\n")
	fmt.Fprintf(&b, "Slab Addr: %d\n", uintptr(unsafe.Pointer(s)))
	fmt.Fprintf(&b, "Object Size: %d\n", objSize)
	fmt.Fprintf(&b, "Objects Per Slab: %d\n", bitSetLen)

	for i := 0; i < len(bitSetBytes); i++ {
		fmt.Fprintf(&b, "bitSet[%d]: % 08b ", i, bitSetBytes[i])
		fmt.Fprintf(&b, "\n")
	}

	for i := uint(0); i < bitSetLen; i++ {
		fmt.Fprintf(&b, "% 03d\n", s.getObjByIdx(i))
	}
	return b.String()
}

// newSlab initializes a new slab based on the given parameters. It can
// potentially error if the memory allocation call fails
// On success the first return value is a pointer to the new slab and the
// second value is nil
// On failure the second returned value is an error
func newSlab(objSize uint8, objsPerSlab uint) (*slab, error) {
	bitSet := bitset.New(objsPerSlab)

	bitSetDataLen := len(bitSet.Bytes()) * 8

	// 1 byte for the objSize, that's a uint8
	// sizeOfBitSet is the BitSet, excluding the data used by its data slice
	// bitSetDataLen is the data used by the BitSets data slice
	// the object slots take up (object size * object count) bytes
	totalLen := 1 + int(sizeOfBitSet) + bitSetDataLen + int(objSize)*int(objsPerSlab)
	data, err := syscall.Mmap(-1, 0, totalLen, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_ANON|syscall.MAP_PRIVATE)
	if err != nil {
		return nil, err
	}

	// set the objSize property of the new slab
	data[0] = byte(objSize)

	// create temporary byte slice that accesses bitSet as underlying data,
	// that way we can read the BitSet like a byte slice
	var copyFrom []byte
	copyFromHeader := (*reflect.SliceHeader)(unsafe.Pointer(&copyFrom))
	copyFromHeader.Data = uintptr(unsafe.Pointer(bitSet))
	copyFromHeader.Cap = int(sizeOfBitSet)
	copyFromHeader.Len = int(sizeOfBitSet)

	// copy the BitSet data structure into memory area at offset 1
	copy(data[1:], copyFrom)

	// get the byte slice header of BitSets data property
	bitSetDataSlice := (*reflect.SliceHeader)(unsafe.Pointer(&data[1+offsetOfBitSetData]))

	// set the data pointer to point at the address right after the BitSet instance
	bitSetDataSlice.Data = uintptr(unsafe.Pointer(&data[1+int(sizeOfBitSet)]))

	// return the data byte slice converted to a slab pointer
	return (*slab)(unsafe.Pointer(&data[0])), nil
}

// addr returns this slabs' address as a SlabAddr type
func (s *slab) addr() SlabAddr {
	return SlabAddr(unsafe.Pointer(s))
}

// bitSet returns this slabs' BitSet as a pointer
func (s *slab) bitSet() *bitset.BitSet {
	return (*bitset.BitSet)(unsafe.Pointer(uintptr(unsafe.Pointer(s)) + 1))
}

// objsPerSlab returns the max number of objects each slab can contain
func (s *slab) objsPerSlab() uint {
	return s.bitSet().Len()
}

// getTotalLength returns the total size of this slab in bytes
func (s *slab) getTotalLength() uintptr {
	return s.getDataOffset() + uintptr(s.objSize)*uintptr(s.objsPerSlab())
}

// getDataOffset returns the offset at which the stored objects start
func (s *slab) getDataOffset() uintptr {
	// multiply the BitSet bytes by 8 because it returns a slice of uint64
	return uintptr(1) + sizeOfBitSet + uintptr(len(s.bitSet().Bytes())*8)
}

// getObjOffset returns the offset at which the object
// at the given index is written
func (s *slab) getObjOffset(idx uint) uintptr {
	// offset where the object data begins
	dataOffset := s.getDataOffset()

	// offset where the object is within the data range
	objectOffset := uintptr(s.objSize) * uintptr(idx)

	return dataOffset + objectOffset
}

// getObjIdx takes an object address and returns the object index
// within this slice
func (s *slab) getObjIdx(obj ObjAddr) uint {
	// offset where the slices object data begins
	dataOffset := uintptr(1) + sizeOfBitSet + uintptr(len(s.bitSet().Bytes())*8)

	// offset where the object is within the data range
	objectOffset := obj - dataOffset - uintptr(unsafe.Pointer(s))

	// calculate index based on object offset and object size
	return uint(objectOffset / uintptr(s.objSize))
}

// addObj takes an object and adds it to this slice if there is
// free space for it
// On success the first return value is the ObjAddr of the newly
// added object, the second value is a bool that indicates if
// the slab is full, the third value indicates success
// On failure the third return value is false, otherwise it's true
func (s *slab) addObj(obj []byte, idx uint) (ObjAddr, bool, bool) {
	offset := s.getObjOffset(idx)

	// objAddr is used as the unique identifier of the newly created object
	objAddr := uintptr(unsafe.Pointer(s)) + offset

	len := uintptr(len(obj))
	src := (*reflect.SliceHeader)(unsafe.Pointer(&obj)).Data

	var i uintptr
	// if length is more than 8 we simply copy as uint64 one-by-one in 8byte chunks
	for ; i+8 <= len; i = i + 8 {
		*(*uint64)(unsafe.Pointer(objAddr + i)) = *(*uint64)(unsafe.Pointer(src + i))
	}

	// if the length is not divisible by 8 we need to copy the left over data
	remainder := len % 8
	if remainder != 0 {
		*((*uint64)(unsafe.Pointer(objAddr + i))) |= (*((*uint64)(unsafe.Pointer(src + i))) & (math.MaxUint64 >> ((8 - remainder) * 8)))
	}

	// set the according object slot as used
	bitSet := s.bitSet()
	bitSet.Set(idx)

	return objAddr, bitSet.All(), true
}

// delete deletes the object at the given object address
// it returns a boolean which indicates if after this delete the slab is empty or not
// on true it is empty, otherwise there is still some data in it
func (s *slab) delete(obj ObjAddr) bool {
	idx := s.getObjIdx(obj)
	bitSet := s.bitSet()
	bitSet.Clear(idx)
	return bitSet.None()
}

// getObjByIdx returns the object at the given index as a byte slice
func (s *slab) getObjByIdx(idx uint) []byte {
	return objFromObjAddr(uintptr(unsafe.Pointer(s))+s.getObjOffset(idx), s.objSize)
}
