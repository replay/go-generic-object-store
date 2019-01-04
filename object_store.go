package main

import "fmt"

type ObjectStore struct {
	slabPools map[uint8]sizedPool
}

// Add will add an object to the slab pool of the correct size
// it returns the objectID of the added object on success
// in case of failure error will be set to an error message
func (o *ObjectStore) Add(obj []byte) (objectID, error) {
	res := objectID{}

	// we only deal with objects up to a size of 255
	if len(obj) == 0 || len(obj) > 255 {
		return res, fmt.Errorf("Add failed: Size of object (%d) is outside limits (1-%d)", len(obj), 255)
	}

	size := uint8(len(obj))

	// we want to index by size, but we'll never store objects of size 0, so we just index by size-1
	pool, ok := o.slabPools[size]
	if !ok {
		o.addSlabPool(size)
		pool = o.slabPools[size]
	}

	var err error
	res, err = pool.add(obj)
	if err != nil {
		return res, err
	}

	return res, nil
}

// addSlabPool adds a slab pool of the specified size to this object store
func (o *ObjectStore) addSlabPool(size uint8) {
	o.slabPools[size] = sizedPool{objSize: size}
}

// Search searches for the given value in the accordingly sized slab pool
// it returns the objectID and true in case of success
// if the search fails the second returned value is false
func (o *ObjectStore) Search(searching []byte) (objectID, bool) {
	size := uint8(len(searching))
	pool, ok := o.slabPools[size]
	if !ok {
		return objectID{}, false
	}

	res, success := pool.search(searching)
	if !success {
		return objectID{}, false
	}
	res.size = size

	return res, true
}

// Get retrieves a value by objectID
// on success the first returned value is object as byte slice and the second is true
// on failure the second returned value is false
func (o *ObjectStore) Get(obj objectID) ([]byte, bool) {
	pool, ok := o.slabPools[obj.size]
	if !ok {
		return nil, false
	}
	return pool.get(obj)
}

// Delete deletes an object by objectID
// on success it returns nil, otherwise it returns an error message
func (o *ObjectStore) Delete(obj objectID) error {
	pool, ok := o.slabPools[obj.size]
	if !ok {
		return fmt.Errorf("Delete failed: slab pool for size %d does not exist", obj.size)
	}
	return pool.delete(obj)
}
