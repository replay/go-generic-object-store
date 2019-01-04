package main

import (
	"reflect"
	"syscall"
)

var objectsPerSlab uint8 = 10

type slab struct {
	free freeList
	data []byte
}
type objectID struct {
	slabAddr  uintptr
	objectPos uint8
}

type sizedPool struct {
	slabs   []slab
	objSize uint8
}

// add adds an object to the pool
// it will try to find a free object slot for the given object, to avoid
// unnecessary allocations. if it can't find a free slot, it will add a
// slab and then use that one.
// the first return value is the objectID of the added object,
// the second value is the error in case one occurred
func (s *sizedPool) add(obj []byte) (objectID, error) {
	var pos uint8
	var success bool
	var slabId int
	for i, s := range s.slabs {
		pos, success = s.free.getFree()
		if success {
			slabId = i
			break
		}
	}
	id := objectID{
		objectPos: pos,
	}
	var err error
	if !success {
		slabId, err = s.addSlab()
		if err != nil {
			return id, err
		}
	}

	slab := s.slabs[slabId]
	slab.free.setUsed(pos)
	offset := int(pos) * int(s.objSize)
	for i := 0; i < int(s.objSize); i++ {
		slab.data[i+offset] = obj[i]
	}

	id.slabAddr = reflect.ValueOf(slab.data).Pointer()
	return id, nil
}

// search searches for a byte slice that must have the length of
// the slab's objectSize.
// When found it returns the objectID and true,
// otherwise the second returned value will be false
func (s *sizedPool) search(searching []byte) (objectID, bool) {
	var res objectID
	if len(searching) != int(s.objSize) {
		return res, false
	}

	for _, slab := range s.slabs {
		offset := 0
		objSize := int(s.objSize)
	OBJECT:
		for i := uint8(0); i < objectsPerSlab; i++ {
			if slab.free.isUsed(i) {
				offset = int(i) * objSize
				obj := slab.data[offset : offset+objSize]
				for j := uint8(0); j < s.objSize; j++ {
					if obj[j] != searching[j] {
						continue OBJECT
					}
				}
				res.objectPos = i
				res.slabAddr = reflect.ValueOf(slab.data).Pointer()
				return res, true
			}
		}
	}
	return res, false
}

// get retreives and object of the given objectID
// the second returned value is true if the object was found,
// otherwise it's false
func (s *sizedPool) get(obj objectID) ([]byte, bool) {
	// verify that the objectPos is inside the valid range
	if obj.objectPos >= objectsPerSlab {
		return nil, false
	}

	slabId := s.getSlabId(obj.slabAddr)
	if slabId < 0 {
		return nil, false
	}

	slab := s.slabs[slabId]
	if !slab.free.isUsed(obj.objectPos) {
		return nil, false
	}

	offset := int(obj.objectPos) * int(s.objSize)

	return slab.data[offset : offset+int(s.objSize)], true
}

// addSlab adds another slab to the pool and initalizes the related structs
func (s *sizedPool) addSlab() (int, error) {
	data, err := syscall.Mmap(-1, 0, int(s.objSize)*int(objectsPerSlab), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_ANON|syscall.MAP_PRIVATE)
	if err != nil {
		return 0, err
	}
	s.slabs = append(s.slabs, slab{
		data: data,
		free: newFreeList(objectsPerSlab),
	})
	return len(s.slabs) - 1, nil
}

// delete takes an objectID and deletes it from the sizedPool
// on success it returns true, otherwise false
func (s *sizedPool) delete(obj objectID) bool {
	slabId := s.getSlabId(obj.slabAddr)
	if slabId < 0 {
		return false
	}

	slab := s.slabs[slabId]

	// verify that the given arguments refer to an object within the size of the data slice
	if len(slab.data) <= int(obj.objectPos)*int(s.objSize) {
		return false
	}

	slab.free.setFree(obj.objectPos)
	return true
}

// getSlabId looks up the ID for the slab referred to by a given pointer
// the pointer must point at the first element of the slabs data slice
func (s *sizedPool) getSlabId(addr uintptr) int {
	for i, s := range s.slabs {
		if reflect.ValueOf(s.data).Pointer() == addr {
			return i
		}
	}
	return -1
}
