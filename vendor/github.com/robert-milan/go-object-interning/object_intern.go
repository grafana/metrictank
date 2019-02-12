package goi

import (
	"fmt"
	"reflect"
	"sync"
	"unsafe"

	gos "github.com/replay/go-generic-object-store"
	"github.com/tmthrgd/shoco"
)

// ObjectIntern stores a map of uintptrs to interned objects.
// The string key is itself using an interned object for its data pointer
type ObjectIntern struct {
	sync.RWMutex
	conf       *ObjectInternConfig
	Store      gos.ObjectStore
	ObjIndex   map[string]uintptr
	compress   func(in []byte) []byte
	decompress func(in []byte) ([]byte, error)
}

// NewObjectIntern returns a new ObjectIntern. Pass in
// nil if you want to use the default configuration, otherwise
// pass in a custom configuration
func NewObjectIntern(c *ObjectInternConfig) *ObjectIntern {
	oi := ObjectIntern{
		conf:     Config,
		Store:    gos.NewObjectStore(100),
		ObjIndex: make(map[string]uintptr),
	}
	if c != nil {
		oi.conf = c
	}

	// set compression and decompression functions
	switch oi.conf.Compression {
	case Shoco:
		oi.compress = shoco.Compress
		oi.decompress = shoco.Decompress
	case ShocoDict:
		panic("Compression ShocoDict not implemented yet")
	case None:
		oi.compress = func(in []byte) []byte { return in }
		oi.decompress = func(in []byte) ([]byte, error) { return in, nil }
	default:
		panic(fmt.Sprintf("Compression %d not recognized", oi.conf.Compression))
	}

	return &oi
}

// CompressionFunc returns the current compression func used by the library
func (oi *ObjectIntern) CompressionFunc() func(in []byte) []byte {
	return oi.compress
}

// DecompressionFunc returns the current decompression func used by the library
func (oi *ObjectIntern) DecompressionFunc() func(in []byte) ([]byte, error) {
	return oi.decompress
}

// Compress returns a compressed version of in as a []byte
// It is important to keep in mind that not all values can be compressed,
// so this may at times return the original value
func (oi *ObjectIntern) Compress(in []byte) []byte {
	return oi.compress(in)
}

// Decompress returns a decompressed version of in as a []byte and nil on success.
// On failure it returns an error
func (oi *ObjectIntern) Decompress(in []byte) ([]byte, error) {
	return oi.decompress(in)
}

// CompressString returns a compressed version of in as a string
// It is important to keep in mind that not all values can be compressed,
// so this may at times return the original value
func (oi *ObjectIntern) CompressString(in string) string {
	if oi.conf.Compression == None {
		return in
	}
	return string(oi.compress([]byte(in)))
}

// DecompressString returns a decompressed version of string as a string and nil upon success.
// On failure it returns in and an error.
func (oi *ObjectIntern) DecompressString(in string) (string, error) {
	if oi.conf.Compression == None {
		return in, nil
	}
	b, err := oi.decompress([]byte(in))
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// AddOrGet finds or adds and then returns a uintptr to an object and nil.
// This method assumes that compression is turned off.
// On failure it returns 0 and an error
//
// If the object is found in the store its reference count is increased by 1.
// If the object is added to the store its reference count is set to 1.
func (oi *ObjectIntern) AddOrGet(obj []byte) (uintptr, error) {
	var addr gos.ObjAddr
	var ok bool
	var err error
	var objString string

	// create a copy so we don't modify the original, especially because compression is turned off
	// when compression is turned on using SHOCO it will return a new slice, which is not the case here
	objComp := make([]byte, len(obj))
	copy(objComp, obj)
	objString = string(obj)

	// acquire lock
	oi.Lock()

	// try to find the object in the index
	addr, ok = oi.ObjIndex[objString]
	if ok {
		// increment reference count by 1
		(*(*uint32)(unsafe.Pointer(addr + uintptr(len(objComp)))))++
		oi.Unlock()
		return addr, nil
	}

	// The object is not in the index therefore it is not in the store.
	// We need to set its initial reference count to 1 before adding it
	objComp = append(objComp, []byte{0x1, 0x0, 0x0, 0x0}...)
	addr, err = oi.Store.Add(objComp)
	if err != nil {
		oi.Unlock()
		return 0, err
	}

	// set objString data to the object inside the object store
	((*reflect.StringHeader)(unsafe.Pointer(&objString))).Data = addr

	// add the object to the index
	oi.ObjIndex[objString] = addr

	oi.Unlock()
	return addr, nil
}

// AddOrGetCompressed finds or adds and then returns a uintptr to an object and nil.
// On failure it returns 0 and an error
//
// If the object is found in the store its reference count is increased by 1.
// If the object is added to the store its reference count is set to 1.
func (oi *ObjectIntern) AddOrGetCompressed(obj []byte) (uintptr, error) {
	var addr gos.ObjAddr
	var ok bool
	var err error
	var objString string

	// compress the object before searching for it
	objComp := oi.compress(obj)
	// if compression is turned off we don't want to work on the original
	if oi.conf.Compression == None {
		objComp := make([]byte, len(obj))
		copy(objComp, obj)
	}
	objString = string(objComp)

	// acquire lock
	oi.Lock()

	// try to find the object in the index
	addr, ok = oi.ObjIndex[objString]
	if ok {
		// increment reference count by 1
		(*(*uint32)(unsafe.Pointer(addr + uintptr(len(objComp)))))++
		oi.Unlock()
		return addr, nil
	}

	// The object is not in the index therefore it is not in the store.
	// We need to set its initial reference count to 1 before adding it
	objComp = append(objComp, []byte{0x1, 0x0, 0x0, 0x0}...)
	addr, err = oi.Store.Add(objComp)
	if err != nil {
		oi.Unlock()
		return 0, err
	}

	// set objString data to the object inside the object store
	((*reflect.StringHeader)(unsafe.Pointer(&objString))).Data = addr

	// add the object to the index
	oi.ObjIndex[objString] = addr

	oi.Unlock()
	return addr, nil
}

// AddOrGetString finds or adds an object and then returns a string with its Data pointer set to the newly interned object and nil.
// This method assumes that compression is turned off.
// On failure it returns an empty string and an error
//
// If the object is found in the store its reference count is increased by 1.
// If the object is added to the store its reference count is set to 1.
func (oi *ObjectIntern) AddOrGetString(obj []byte) (string, error) {
	var addr gos.ObjAddr
	var ok bool
	var err error
	var objString string

	// create a copy so we don't modify the original, especially because compression is turned off
	// when compression is turned on using SHOCO it will return a new slice, which is not the case here
	objComp := make([]byte, len(obj))
	copy(objComp, obj)
	objString = string(obj)

	// acquire lock
	oi.Lock()

	// try to find the object in the index
	addr, ok = oi.ObjIndex[objString]
	if ok {
		// increment reference count by 1
		(*(*uint32)(unsafe.Pointer(addr + uintptr(len(objComp)))))++
		tmpString := objString
		StringHeader := (*reflect.StringHeader)(unsafe.Pointer(&tmpString))
		StringHeader.Data = addr
		oi.Unlock()
		return tmpString, nil
	}

	// The object is not in the index therefore it is not in the store.
	// We need to set its initial reference count to 1 before adding it
	objComp = append(objComp, []byte{0x1, 0x0, 0x0, 0x0}...)
	addr, err = oi.Store.Add(objComp)
	if err != nil {
		oi.Unlock()
		return "", err
	}

	// set objString data to the object inside the object store
	((*reflect.StringHeader)(unsafe.Pointer(&objString))).Data = addr

	// add the object to the index
	oi.ObjIndex[objString] = addr

	tmpString := objString
	StringHeader := (*reflect.StringHeader)(unsafe.Pointer(&tmpString))
	StringHeader.Data = addr

	oi.Unlock()
	return tmpString, nil
}

// AddOrGetStringCompressed finds or adds an object and then returns a string with its Data pointer set to the newly interned object and nil.
// On failure it returns an empty string and an error
//
// If the object is found in the store its reference count is increased by 1.
// If the object is added to the store its reference count is set to 1.
func (oi *ObjectIntern) AddOrGetStringCompressed(obj []byte) (string, error) {
	var addr gos.ObjAddr
	var ok bool
	var err error
	var objString string

	// compress the object before searching for it
	objComp := oi.compress(obj)
	// if compression is turned off we don't want to work on the original
	if oi.conf.Compression == None {
		objComp := make([]byte, len(obj))
		copy(objComp, obj)
	}
	objString = string(objComp)

	// acquire lock
	oi.Lock()

	// try to find the object in the index
	addr, ok = oi.ObjIndex[objString]
	if ok {
		// increment reference count by 1
		(*(*uint32)(unsafe.Pointer(addr + uintptr(len(objComp)))))++
		tmpString := objString
		StringHeader := (*reflect.StringHeader)(unsafe.Pointer(&tmpString))
		StringHeader.Data = addr
		oi.Unlock()
		return tmpString, nil
	}

	// The object is not in the index therefore it is not in the store.
	// We need to set its initial reference count to 1 before adding it
	objComp = append(objComp, []byte{0x1, 0x0, 0x0, 0x0}...)
	addr, err = oi.Store.Add(objComp)
	if err != nil {
		oi.Unlock()
		return "", err
	}

	// set objString data to the object inside the object store
	((*reflect.StringHeader)(unsafe.Pointer(&objString))).Data = addr

	// add the object to the index
	oi.ObjIndex[objString] = addr

	tmpString := objString
	StringHeader := (*reflect.StringHeader)(unsafe.Pointer(&tmpString))
	StringHeader.Data = addr

	oi.Unlock()
	return tmpString, nil
}

// GetNoRefCntCompressed finds an interned object and returns its address as a uintptr.
// Upon failure it returns 0 and an error.
//
// This method is designed specifically to be used with map keys that are interned,
// since the only way to retrieve the key itself is by iterating over the entire map.
// This method should be faster than iterating over a map (depending on the size of the map).
// This is usually called directly before deleting an interned map key from its map so that we
// can properly decrement the reference count of that interned object.
func (oi *ObjectIntern) GetNoRefCntCompressed(obj []byte) (uintptr, error) {
	var addr gos.ObjAddr
	var ok bool
	var objString string

	// compress the object before searching for it
	objComp := oi.compress(obj)
	objString = string(objComp)

	// acquire read lock
	oi.RLock()

	// try to find the object in the index
	addr, ok = oi.ObjIndex[objString]
	if ok {
		oi.RUnlock()
		return addr, nil
	}

	oi.RUnlock()
	return 0, fmt.Errorf("Could not find object in store")
}

// GetNoRefCntString returns an interned version of a string stored at objAddr
// Upon failure it returns an empty string and an error
//
// This method does not increase the reference count of the interned object
func (oi *ObjectIntern) GetNoRefCntString(objAddr uintptr) (string, error) {
	oi.RLock()
	defer oi.RUnlock()

	b, err := oi.Store.Get(objAddr)
	if err != nil {
		return "", err
	}

	var tmpString string
	StringHeader := (*reflect.StringHeader)(unsafe.Pointer(&tmpString))
	StringHeader.Data = objAddr
	// remove 4 trailing bytes for reference count
	StringHeader.Len = len(b) - 4

	return tmpString, nil
}

// Delete decrements the reference count of an object identified by its address.
// Possible return values are as follows:
//
// true, nil - reference count reached 0 and the object was removed from both the index
// and the object store.
//
// false, nil - reference count was decremented by 1 and no further action was taken.
//
// false, error - the object was not found in the object store or could not be deleted
func (oi *ObjectIntern) Delete(objAddr uintptr) (bool, error) {
	var compObj []byte
	var err error

	// acquire write lock
	oi.Lock()

	// check if object exists in the object store
	compObj, err = oi.Store.Get(objAddr)
	if err != nil {
		oi.Unlock()
		return false, err
	}

	// most likely case is that we will just decrement the reference count and return
	if *(*uint32)(unsafe.Pointer(objAddr + uintptr(len(compObj)-4))) > 1 {
		// decrement reference count by 1
		(*(*uint32)(unsafe.Pointer(objAddr + uintptr(len(compObj)-4))))--

		oi.Unlock()
		return false, nil
	}

	// if reference count is 1, delete the object and remove it from index
	// If one of these operations fails it is still safe to perform the other
	// Once we get to this point we are just going to remove all traces of the object

	// delete object from index first
	// If you delete all of the objects in the slab then the slab will be deleted
	// When this happens the memory that the slab was using is MUnmapped, which is
	// the same memory pointed to by the key stored in the ObjIndex. When you try to
	// access the key to delete it from the ObjIndex you will get a SEGFAULT
	//
	// remove 4 trailing bytes for reference count since ObjIndex does not store reference count in the key
	delete(oi.ObjIndex, string(compObj[:len(compObj)-4]))

	// delete object from object store
	err = oi.Store.Delete(objAddr)

	if err == nil {
		oi.Unlock()
		return true, nil
	}
	oi.Unlock()
	return false, err
}

// DeleteByVal decrements the reference count of an object identified by its value as a []byte.
// This method assumes that compression is turned off.
// Possible return values are as follows:
//
// true, nil - reference count reached 0 and the object was removed from both the index
// and the object store.
//
// false, nil - reference count was decremented by 1 and no further action was taken.
//
// false, error - the object was not found in the object store or could not be deleted
func (oi *ObjectIntern) DeleteByVal(obj []byte) (bool, error) {
	var addr gos.ObjAddr
	var ok bool
	var objString string

	objString = string(obj)

	// acquire read lock
	oi.RLock()

	// try to find the object in the index
	addr, ok = oi.ObjIndex[objString]
	if !ok {
		oi.RUnlock()
		return false, fmt.Errorf("Could not find object in store")
	}

	oi.RUnlock()
	return oi.Delete(addr)
}

// DeleteByValCompressed decrements the reference count of an object identified by its value as a []byte.
// Possible return values are as follows:
//
// true, nil - reference count reached 0 and the object was removed from both the index
// and the object store.
//
// false, nil - reference count was decremented by 1 and no further action was taken.
//
// false, error - the object was not found in the object store or could not be deleted
func (oi *ObjectIntern) DeleteByValCompressed(obj []byte) (bool, error) {
	var addr gos.ObjAddr
	var ok bool
	var objString string

	// compress the object before searching for it
	objComp := oi.compress(obj)
	objString = string(objComp)

	// acquire read lock
	oi.RLock()

	// try to find the object in the index
	addr, ok = oi.ObjIndex[objString]
	if !ok {
		oi.RUnlock()
		return false, fmt.Errorf("Could not find object in store")
	}

	oi.RUnlock()
	return oi.Delete(addr)
}

// DeleteByValString decrements the reference count of an object identified by its string representation.
// This method assumes that compression is turned off.
//
// Possible return values are as follows:
//
// true, nil - reference count reached 0 and the object was removed from both the index
// and the object store.
//
// false, nil - reference count was decremented by 1 and no further action was taken.
//
// false, error - the object was not found in the object store or could not be deleted
func (oi *ObjectIntern) DeleteByValString(obj string) (bool, error) {
	var addr gos.ObjAddr
	var ok bool

	// acquire read lock
	oi.RLock()

	// try to find the object in the index
	addr, ok = oi.ObjIndex[obj]
	if !ok {
		oi.RUnlock()
		return false, fmt.Errorf("Could not find object in store")
	}

	oi.RUnlock()
	return oi.Delete(addr)
}

// DeleteByValStringCompressed decrements the reference count of an object identified by its string representation.
//
// Possible return values are as follows:
//
// true, nil - reference count reached 0 and the object was removed from both the index
// and the object store.
//
// false, nil - reference count was decremented by 1 and no further action was taken.
//
// false, error - the object was not found in the object store or could not be deleted
func (oi *ObjectIntern) DeleteByValStringCompressed(obj string) (bool, error) {
	var addr gos.ObjAddr
	var ok bool

	b, err := oi.decompress([]byte(obj))
	if err != nil {
		return false, err
	}
	objString := string(b)

	// acquire read lock
	oi.RLock()

	// try to find the object in the index
	addr, ok = oi.ObjIndex[objString]
	if !ok {
		oi.RUnlock()
		return false, fmt.Errorf("Could not find object in store")
	}

	oi.RUnlock()
	return oi.Delete(addr)
}

// RefCnt checks if the object identified by objAddr exists in the
// object store and returns its current reference count and nil on success.
// On failure it returns 0 and an error, which means the object was not found
// in the object store.
func (oi *ObjectIntern) RefCnt(objAddr uintptr) (uint32, error) {
	oi.RLock()
	defer oi.RUnlock()

	// check if object exists in the object store
	compObj, err := oi.Store.Get(objAddr)
	if err != nil {
		return 0, err
	}

	// remove 4 trailing bytes for reference count
	return *(*uint32)(unsafe.Pointer(objAddr + uintptr(len(compObj)-4))), nil

}

// ObjBytes returns a []byte and nil on success.
// On failure it returns nil and an error.
//
// If compression is used, this method does not use the interned data to create a []byte,
// instead it allocates a new []byte.
//
// If compression is turned off, this will return a []byte slice with the backing array
// set to the interned data.
func (oi *ObjectIntern) ObjBytes(objAddr uintptr) ([]byte, error) {
	oi.RLock()
	defer oi.RUnlock()

	b, err := oi.Store.Get(objAddr)
	if err != nil {
		return nil, err
	}
	// remove 4 trailing bytes for reference count and decompress
	objDecomp, err := oi.decompress(b[:len(b)-4])
	if err != nil {
		return nil, err
	}
	return objDecomp, nil
}

// ObjString returns a string and nil on success.
// On failure it returns an empty string and an error.
//
// This method does not use the interned data to create a string,
// instead it allocates a new string.
func (oi *ObjectIntern) ObjString(objAddr uintptr) (string, error) {
	oi.RLock()
	defer oi.RUnlock()

	b, err := oi.Store.Get(objAddr)
	if err != nil {
		return "", err
	}
	// remove 4 trailing bytes for reference count and decompress
	objDecomp, err := oi.decompress(b[:len(b)-4])
	if err != nil {
		return "", err
	}
	return string(objDecomp), nil
}

// SetString takes an object adress and a string header, it assumes that compression is turned off.
// Upon success it sets the string to point at objAddr with the proper length set.
// On failure it takes no action and returns false.
func (oi *ObjectIntern) SetString(objAddr uintptr, StringHdr *reflect.StringHeader) bool {
	oi.RLock()
	defer oi.RUnlock()

	b, err := oi.Store.Get(objAddr)
	if err != nil {
		return false
	}

	StringHdr.Data = objAddr
	StringHdr.Len = len(b) - 4
	return true
}

// SetStringCompressed takes an object adress and a string header.
// Upon success it sets the string to point at objAddr with the proper length set.
// On failure it takes no action and returns false.
func (oi *ObjectIntern) SetStringCompressed(objAddr uintptr, StringHdr *reflect.StringHeader) bool {
	oi.RLock()
	defer oi.RUnlock()

	b, err := oi.Store.Get(objAddr)
	if err != nil {
		return false
	}

	objDecomp, err := oi.decompress(b[:len(b)-4])
	if err != nil {
		return false
	}

	StringHdr.Data = objAddr
	StringHdr.Len = len(objDecomp)
	return true
}

// Len takes a slice of object addresses, it assumes that compression is turned off.
// Upon success it returns a slice of the lengths of all of the interned objects - the 4 trailing bytes for reference count, and true.
// The returned slice indexes match the indexes of the slice of uintptrs.
// On failure it returns a possibly partial slice of the lengths, and false.
func (oi *ObjectIntern) Len(ptrs []uintptr) (retLn []int, all bool) {
	retLn = make([]int, len(ptrs))
	all = true

	oi.RLock()
	defer oi.RUnlock()

	for idx, ptr := range ptrs {
		b, err := oi.Store.Get(ptr)
		if err != nil {
			return retLn, false
		}
		// remove 4 trailing bytes of reference count
		retLn[idx] = len(b) - 4
	}
	return
}
