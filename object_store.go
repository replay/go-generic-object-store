package gos

import (
	"fmt"
	"reflect"
	"sort"
	"unsafe"
)

// ObjectStore contains a map of slabPools indexed by the size of the objects stored in each pool
// It also contains a lookup table which is a slice of slabInfos
// lookupTable is kept sorted in descending order and updated whenever a slab is created or deleted
type ObjectStore struct {
	slabPools   map[uint8]*slabPool
	lookupTable []SlabAddr
	objsPerSlab uint
}

func NewObjectStore(objsPerSlab uint) ObjectStore {
	return ObjectStore{
		objsPerSlab: objsPerSlab,
		slabPools:   make(map[uint8]*slabPool),
	}
}

// ObjAddr is a uintptr used for storing addresses of objects in slabs
type ObjAddr = uintptr

// SlabAddr is a uintptr used for storing memory addresses of &slab.data[0] in each slab
type SlabAddr = uintptr

// Add will add an object to the slab pool of the correct size
// on success it returns the memory address as an ObjAddr (uintptr) of the added object
// on failure it returns 0 and an error
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

	// try to add the object to a slab in the pool
	var err error
	oAddr, sAddr, err = pool.add(obj)
	if err != nil {
		return 0, err
	}

	// when sAddr != 0 this indicates that a new slab was created while adding the object
	// we must update or lookup table to track the new slab
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
// on success it returns the object address and true
// on failure it returns 0 and false
func (o *ObjectStore) Search(searching []byte) (ObjAddr, bool) {
	var obj ObjAddr

	size := uint8(len(searching))
	pool, ok := o.slabPools[size]
	if !ok {
		return 0, false
	}

	obj, success := pool.search(searching)
	if !success {
		return 0, false
	}

	return obj, true
}

// Get retrieves a value by object address
// on success returns a []byte of appropriate length of the requested object
// on failure returns nil
func (o *ObjectStore) Get(obj ObjAddr) ([]byte, error) {
	sAddr, err := o.getSlabAddress(obj)
	if err != nil {
		return nil, err
	}
	size := *(*uint8)(unsafe.Pointer(sAddr))
	var res []byte
	resHeader := (*reflect.SliceHeader)(unsafe.Pointer(&res))
	resHeader.Data = obj
	resHeader.Len = int(size)
	resHeader.Cap = int(size)
	return res, nil
}

// Delete deletes an object by object address
// on success it returns nil, otherwise it returns an error message
/*func (o *ObjectStore) Delete(obj ObjAddr) error {
	idx, err := o.getObjectSize(obj)
	if err != nil {
		return err
	}

	slab := o.lookupTable[idx]
	if !ok {
		return fmt.Errorf("ObjectStore: Delete failed slab pool for size %d does not exist", o.lookupTable[idx].size)
	}
	return pool.delete(obj)
}*/

// getObjectSize searches, in a descending order sorted slice, for a slab which is likely to contain
// the object identified by its address
// on success it returns the index position and nil
// on failure it returns 0 and an error
func (o *ObjectStore) getSlabAddress(obj ObjAddr) (SlabAddr, error) {
	idx := sort.Search(len(o.lookupTable), func(i int) bool { return o.lookupTable[i] <= obj })
	ok := idx < len(o.lookupTable) && idx >= 0
	if !ok {
		return 0, fmt.Errorf("ObjectStore: getSlabAddr failed to locate size for the object address")
	}
	return o.lookupTable[idx], nil
}
