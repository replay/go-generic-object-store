package gos

import (
	"reflect"
	"sort"
	"syscall"
	"unsafe"
)

var objectsPerSlab uint8 = 100

type slabPool struct {
	slabs       []*slab
	objSize     uint8
	objsPerSlab uint
}

func NewSlabPool(objSize uint8, objsPerSlab uint) *slabPool {
	return &slabPool{
		objSize:     objSize,
		objsPerSlab: objsPerSlab,
	}
}

// add adds an object to the pool
// it will try to find a free object slot for the given object, to avoid
// unnecessary allocations. if it can't find a free slot, it will add a
// slab and then use that one.
// the first return value is the ObjAddr of the added object.
// the second value is set to the slab address if the call created a new slab
// if no new slab has been created, then the second value is 0.
// the third value is nil if there was no error, otherwise it is the error
func (s *slabPool) add(obj []byte) (ObjAddr, SlabAddr, error) {
	var success bool
	var idx uint
	var currentSlab *slab
	var slabId int

	for slabId, currentSlab = range s.slabs {
		idx, success = currentSlab.bitSet().NextClear(0)
		if !success {
			continue
		}
		return currentSlab.addObjByIdx(idx, obj), 0, nil
	}

	var err error
	slabId, err = s.addSlab()
	if err != nil {
		return 0, 0, err
	}

	currentSlab = s.slabs[slabId]
	idx = 0

	return currentSlab.addObjByIdx(idx, obj), currentSlab.addr(), nil
}

// findSlabByObjAddr takes an object address and finds the correct slab
// where this object is in by looking it up from its slab list
// it returns the slab index if the correct slab was found, otherwise
// the return value will be set to the number of known slabs
func (s *slabPool) findSlabByObjAddr(obj ObjAddr) int {
	return sort.Search(len(s.slabs), func(i int) bool { return s.slabs[i].addr() <= obj })
}

// addSlab adds another slab to the pool and initalizes the related structs
// on success the first returned value is the slab index of the added slab
// on failure the second returned value is set to the error message
func (s *slabPool) addSlab() (int, error) {
	addedSlab, err := newSlab(s.objSize, s.objsPerSlab)
	if err != nil {
		return 0, err
	}

	newSlabAddr := addedSlab.addr()

	// find the right location to insert the new slab
	insertAt := sort.Search(len(s.slabs), func(i int) bool { return s.slabs[i].addr() < newSlabAddr })
	s.slabs = append(s.slabs, &slab{})
	copy(s.slabs[insertAt+1:], s.slabs[insertAt:])
	s.slabs[insertAt] = addedSlab

	return insertAt, nil
}

// search searches for a byte slice that must have the length of
// the slab's objectSize.
// When found it returns the object address and true,
// otherwise the second returned value is false
func (s *slabPool) search(searching []byte) (ObjAddr, bool) {
	if len(searching) != int(s.objSize) {
		return 0, false
	}

	for _, currentSlab := range s.slabs {
		objSize := int(s.objSize)

	OBJECT:
		for i := uint(0); i < s.objsPerSlab; i++ {
			if currentSlab.bitSet().Test(i) {
				obj := currentSlab.getObjByIdx(i)
				for j := 0; j < objSize; j++ {
					if obj[j] != searching[j] {
						continue OBJECT
					}
				}
				return ObjAddr(unsafe.Pointer(&obj[0])), true
			}
		}
	}

	return 0, false
}

// get retreives and object of the given object address
func (s *slabPool) get(obj ObjAddr) []byte {
	var res []byte
	resHeader := (*reflect.SliceHeader)(unsafe.Pointer(&res))
	resHeader.Data = obj
	resHeader.Len = int(s.objSize)
	resHeader.Cap = int(s.objSize)
	return res
}

// deleteSlab deletes the slab at the given slab index
// on success it returns nil, otherwise it returns an error
func (s *slabPool) deleteSlab(slabId int) error {
	currentSlab := s.slabs[slabId]

	// delete slab id from slab slice
	copy(s.slabs[slabId:], s.slabs[slabId+1:])
	s.slabs[len(s.slabs)-1] = &slab{}
	s.slabs = s.slabs[:len(s.slabs)-1]

	// unmap the slab's memory
	err := syscall.Munmap(*(*[]byte)(unsafe.Pointer(currentSlab)))
	if err != nil {
		return err
	}

	return nil
}
