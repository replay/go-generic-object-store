package main

import (
	"fmt"
	"sort"
)

// ObjectStore contains a map of sizedPools indexed by the size of the objects stored in each pool
// It also contains a lookup table which is a slice of slabInfos
// lookupTable is kept sorted in descending order and updated whenever a slab is created or deleted
type ObjectStore struct {
	slabPools   map[uint8]sizedPool
	lookupTable []slabInfo
}

type slabInfo struct {
	start SlabAddr
	size  uint8
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
		insertAt := sort.Search(len(o.lookupTable), func(i int) bool { return o.lookupTable[i].start < sAddr })
		o.lookupTable = append(o.lookupTable, slabInfo{})
		copy(o.lookupTable[insertAt+1:], o.lookupTable[insertAt:])
		o.lookupTable[insertAt] = slabInfo{start: sAddr, size: size}
	}

	return oAddr, nil
}

// addSlabPool adds a slab pool of the specified size to this object store
func (o *ObjectStore) addSlabPool(size uint8) {
	o.slabPools[size] = sizedPool{objSize: size}
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
// on success the first returned value is object as byte slice and the second is true
// on failure the second returned value is false
func (o *ObjectStore) Get(obj ObjAddr) ([]byte, bool) {
	idx, err := o.getObjectSize(obj)
	if err != nil {
		return nil, false
	}
	pool, ok := o.slabPools[o.lookupTable[idx].size]
	if !ok {
		return nil, false
	}
	return pool.get(obj)
}

// Delete deletes an object by object address
// on success it returns nil, otherwise it returns an error message
func (o *ObjectStore) Delete(obj ObjAddr) error {
	idx, err := o.getObjectSize(obj)
	if err != nil {
		return err
	}
	pool, ok := o.slabPools[o.lookupTable[idx].size]
	if !ok {
		return fmt.Errorf("ObjectStore: Delete failed slab pool for size %d does not exist", o.lookupTable[idx].size)
	}
	return pool.delete(obj)
}

// getObjectSize searches, in a descending order sorted slice, for a slab which is likely to contain
// the object identified by its address
// on success it returns the index position and nil
// on failure it returns 0 and an error
func (o *ObjectStore) getObjectSize(obj ObjAddr) (int, error) {
	idx := sort.Search(len(o.lookupTable), func(i int) bool { return o.lookupTable[i].start <= obj })
	ok := idx < len(o.lookupTable) && idx >= 0
	if !ok {
		return 0, fmt.Errorf("ObjectStore: getObjectSize failed to locate size for the object address")
	}
	return idx, nil
}
