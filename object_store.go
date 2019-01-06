package gos

import (
	"fmt"
	"reflect"
	"sort"
	"unsafe"
)

// ObjectStore contains a map of slabPools indexed by the size of the objects stored in each pool
// It also contains a lookup table which is a slice of SlabAddr
// lookupTable is kept sorted in descending order and updated whenever a slab is created or deleted
type ObjectStore struct {
	slabPools   map[uint8]*slabPool
	lookupTable []SlabAddr
	objsPerSlab uint
}

// NewObjectStore initializes a new object store with the given number of objects per slab,
// it returns the object store as a value
func NewObjectStore(objsPerSlab uint) ObjectStore {
	return ObjectStore{
		objsPerSlab: objsPerSlab,
		slabPools:   make(map[uint8]*slabPool),
	}
}

// ObjAddr is a uintptr used for storing the addresses of objects in slabs
type ObjAddr = uintptr

// SlabAddr is a uintptr used for storing the memory addresses of slabs
type SlabAddr = uintptr

// slabFromAddr takes a SlabAddr and returns a pointer to the slab
func slabFromSlabAddr(addr SlabAddr) *slab {
	return (*slab)(unsafe.Pointer(addr))
}

// objFromObjAddr takes an ObjAddr and an object size, then it returns the
// object as a byte slice.
// it is important that the size is correct, otherwise anything can happen
func objFromObjAddr(obj ObjAddr, size uint8) []byte {
	var res []byte
	resHeader := (*reflect.SliceHeader)(unsafe.Pointer(&res))
	resHeader.Data = obj
	resHeader.Len = int(size)
	resHeader.Cap = resHeader.Len
	return res
}

// objAddrFromObj takes and object and returns its address as an ObjAddr
func objAddrFromObj(obj []byte) ObjAddr {
	return ObjAddr(unsafe.Pointer(&obj[0]))
}

// Add takes an object and adds it to the slab pool of the correct size
// On success it returns the memory address of the added object as an ObjAddr
// On failure it returns an error as the second value
func (o *ObjectStore) Add(obj []byte) (ObjAddr, error) {
	var oAddr ObjAddr
	var sAddr SlabAddr

	// we only deal with objects up to a size of 255
	if len(obj) == 0 || len(obj) > 255 {
		return 0, fmt.Errorf("ObjectStore: Add failed because size of object (%d) is outside limits (1-%d)", len(obj), 255)
	}

	size := uint8(len(obj))

	// get correct pool based on size of object
	// if not found, create new pool for that size
	pool, ok := o.slabPools[size]
	if !ok {
		o.addSlabPool(size)
		pool = o.slabPools[size]
	}

	// try to add the object to the pool
	// there is potential for an error because this involves memory allocations
	var err error
	oAddr, sAddr, err = pool.add(obj)
	if err != nil {
		return 0, err
	}

	// when sAddr != 0 this indicates that a new slab was created while adding the object
	// we must update our lookup table to track the new slab
	if sAddr != 0 {
		// we keep the lookup table sorted in descending order and insert new entries at an appropriate position
		insertAt := sort.Search(len(o.lookupTable), func(i int) bool { return o.lookupTable[i] < sAddr })
		o.lookupTable = append(o.lookupTable, 0)
		copy(o.lookupTable[insertAt+1:], o.lookupTable[insertAt:])
		o.lookupTable[insertAt] = sAddr
	}

	return oAddr, nil
}

// addSlabPool adds a slab pool of the specified size to this object store
func (o *ObjectStore) addSlabPool(size uint8) {
	o.slabPools[size] = NewSlabPool(size, o.objsPerSlab)
}

// Search searches for the given value in the accordingly sized slab pool
// On success it returns the object address and true
// On failure it returns 0 and false
func (o *ObjectStore) Search(searching []byte) (ObjAddr, bool) {
	var obj ObjAddr

	size := uint8(len(searching))
	pool, ok := o.slabPools[size]
	if !ok {
		// there is no pool for the size of the searched object,
		// so we can directly give up
		return 0, false
	}

	obj, success := pool.search(searching)
	if !success {
		return 0, false
	}

	return obj, true
}

// Get retrieves a value by object address
// On success it returns a byte slice of appropriate length,
// containing the requested object data
// On failure the second returned value is the error
func (o *ObjectStore) Get(obj ObjAddr) ([]byte, error) {
	sAddr, err := o.getSlabAddress(obj)
	if err != nil {
		return nil, err
	}

	slab := slabFromSlabAddr(sAddr)
	return objFromObjAddr(obj, slab.objSize), nil
}

// Delete deletes an object by object address
// On success it returns nil, otherwise it returns an error message
func (o *ObjectStore) Delete(obj ObjAddr) error {
	slabAddr, err := o.getSlabAddress(obj)
	if err != nil {
		return err
	}

	slab := slabFromSlabAddr(slabAddr)
	empty := slab.delete(obj)

	// if true then this slab is empty now, so it can be removed
	if empty {
		err := o.slabPools[slab.objSize].deleteSlab(slabAddr)
		if err != nil {
			return err
		}
	}

	return nil
}

// getObjectSize searches, in a descending order sorted slice, for a slab which is likely to contain
// the object identified by the given address
// On success it returns the slab address as SlabAddr and nil
// On failure it returns 0 and an error
func (o *ObjectStore) getSlabAddress(obj ObjAddr) (SlabAddr, error) {
	idx := sort.Search(len(o.lookupTable), func(i int) bool { return o.lookupTable[i] <= obj })
	ok := idx < len(o.lookupTable) && idx >= 0
	if !ok {
		return 0, fmt.Errorf("ObjectStore: getSlabAddr failed to locate size for the object address")
	}
	return o.lookupTable[idx], nil
}
