package gos

import (
	"fmt"
	"reflect"
	"sort"
	"sync"
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
	var objAddr ObjAddr
	var currentSlab *slab

	for _, currentSlab = range s.slabs {
		objAddr, success = currentSlab.addObj(obj)
		if success {
			return objAddr, 0, nil
		}
	}

	var err error
	currentSlab, err = s.addSlab()
	if err != nil {
		return 0, 0, err
	}

	objAddr, success = currentSlab.addObj(obj)
	if !success {
		return 0, 0, fmt.Errorf("Add: Failed adding object to new slab")
	}

	return objAddr, currentSlab.addr(), nil
}

// findSlabByObjAddr takes an object address and finds the correct slab
// where this object is in by looking it up from its slab list
// it returns the slab index if the correct slab was found, otherwise
// the return value will be set to the number of known slabs
func (s *slabPool) findSlabByAddr(obj uintptr) int {
	return sort.Search(len(s.slabs), func(i int) bool { return s.slabs[i].addr() <= obj })
}

// addSlab adds another slab to the pool and initalizes the related structs
// on success the first returned value is a pointer to the new slab
// on failure the second returned value is set to the error message
func (s *slabPool) addSlab() (*slab, error) {
	addedSlab, err := newSlab(s.objSize, s.objsPerSlab)
	if err != nil {
		return nil, err
	}

	newSlabAddr := addedSlab.addr()

	// find the right location to insert the new slab
	insertAt := sort.Search(len(s.slabs), func(i int) bool { return s.slabs[i].addr() < newSlabAddr })
	s.slabs = append(s.slabs, &slab{})
	copy(s.slabs[insertAt+1:], s.slabs[insertAt:])
	s.slabs[insertAt] = addedSlab

	return addedSlab, nil
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

func (s *slabPool) searchBatched(searching [][]byte) []ObjAddr {
	wg := sync.WaitGroup{}
	resultSet := make([]ObjAddr, len(searching))
	type result struct {
		idx  uint
		addr ObjAddr
	}
	resChan := make(chan result)
	objSize := int(s.objSize)

	wg.Add(len(s.slabs))
	for i := range s.slabs {
		go func(currentSlab *slab) {
			defer wg.Done()

			for j := uint(0); j < s.objsPerSlab; j++ {
				if currentSlab.bitSet().Test(j) {
					storedObj := currentSlab.getObjByIdx(j)

				SEARCH:
					for k, searchedObj := range searching {
						for l := 0; l < objSize; l++ {
							if storedObj[l] != searchedObj[l] {
								continue SEARCH
							}

						}

						resChan <- result{
							idx:  uint(k),
							addr: objAddrFromObj(storedObj),
						}
					}
				}
			}
		}(s.slabs[i])
	}
	go func() {
		wg.Wait()
		close(resChan)
	}()

	for res := range resChan {
		resultSet[res.idx] = res.addr
	}

	return resultSet
}

// get retreives and object of the given object address
func (s *slabPool) get(obj ObjAddr) []byte {
	return objFromObjAddr(obj, s.objSize)
}

// deleteSlab deletes the slab at the given slab index
// on success it returns nil, otherwise it returns an error
func (s *slabPool) deleteSlab(slabAddr SlabAddr) error {
	slabIdx := s.findSlabByAddr(uintptr(slabAddr))

	currentSlab := s.slabs[slabIdx]

	// delete slab id from slab slice
	copy(s.slabs[slabIdx:], s.slabs[slabIdx+1:])
	s.slabs[len(s.slabs)-1] = &slab{}
	s.slabs = s.slabs[:len(s.slabs)-1]

	totalLen := int(currentSlab.getTotalLength())

	// unmap the slab's memory
	var toDelete []byte
	sliceHeader := (*reflect.SliceHeader)(unsafe.Pointer(&toDelete))
	sliceHeader.Data = uintptr(unsafe.Pointer(currentSlab))
	sliceHeader.Len = totalLen
	sliceHeader.Cap = sliceHeader.Len

	err := syscall.Munmap(toDelete)
	if err != nil {
		return err
	}

	return nil
}
