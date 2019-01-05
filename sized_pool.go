package main

import (
	"fmt"
	"reflect"
	"sort"
	"syscall"
	"unsafe"

	"github.com/willf/bitset"
)

var objectsPerSlab uint8 = 100

type slab struct {
	free *bitset.BitSet
	data []byte
}

func (s *slab) addr() SlabAddr {
	return SlabAddr(unsafe.Pointer(&s.data[0]))
}

type slabPool struct {
	slabs       []slab
	objSize     uint8
	objsPerSlab uint8
}

func NewSlabPool(objSize, objsPerSlab uint8) slabPool {
	return slabPool{
		objSize:     objSize,
		objsPerSlab: objsPerSlab,
	}
}

// add adds an object to the pool
// it will try to find a free object slot for the given object, to avoid
// unnecessary allocations. if it can't find a free slot, it will add a
// slab and then use that one.
// the first return value is the objectID of the added object,
// the second value is the error in case one occurred
func (s *slabPool) add(obj []byte) (ObjAddr, SlabAddr, error) {
	var success bool
	var usedSlab slab
	var pos uint
	for slabId, slab := range s.slabs {
		pos, success = slab.free.NextClear(0)
		if success {
			usedSlab = s.slabs[slabId]
			break
		}
	}

	var newSlabAddr SlabAddr
	var newObjAddr ObjAddr

	if !success {
		slabId, err := s.addSlab()
		if err != nil {
			return newObjAddr, newSlabAddr, err
		}
		usedSlab = s.slabs[slabId]
		newSlabAddr = usedSlab.addr()
	}

	usedSlab.free.Set(uint(pos))
	offset := uint16(pos) * uint16(s.objSize)
	newObjAddr = ObjAddr(usedSlab.addr()) + ObjAddr(offset)
	copy(usedSlab.data[offset:], obj)

	return newObjAddr, newSlabAddr, nil
}

func (s *slabPool) findSlabByObjAddr(obj ObjAddr) int {
	return sort.Search(len(s.slabs), func(i int) bool { return s.slabs[i].addr() <= obj })
}

// addSlab adds another slab to the pool and initalizes the related structs
func (s *slabPool) addSlab() (int, error) {
	data, err := syscall.Mmap(-1, 0, int(s.objSize)*int(s.objsPerSlab), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_ANON|syscall.MAP_PRIVATE)
	if err != nil {
		return 0, err
	}
	newSlab := slab{
		data: data,
		free: bitset.New(uint(s.objsPerSlab)),
	}
	newSlabAddr := newSlab.addr()

	// find the right location to insert the new slab
	insertAt := sort.Search(len(s.slabs), func(i int) bool { return s.slabs[i].addr() < newSlabAddr })
	s.slabs = append(s.slabs, slab{})
	copy(s.slabs[insertAt+1:], s.slabs[insertAt:])
	s.slabs[insertAt] = newSlab

	return insertAt, nil
}

// search searches for a byte slice that must have the length of
// the slab's objectSize.
// When found it returns the objectID and true,
// otherwise the second returned value will be false
func (s *slabPool) search(searching []byte) (ObjAddr, bool) {
	if len(searching) != int(s.objSize) {
		return 0, false
	}

	for _, slab := range s.slabs {
		offset := 0
		objSize := int(s.objSize)
	OBJECT:
		for i := uint8(0); i < s.objsPerSlab; i++ {
			if slab.free.Test(uint(i)) {
				offset = int(i) * objSize
				obj := slab.data[offset : offset+objSize]
				for j := uint8(0); j < s.objSize; j++ {
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

// get retreives and object of the given objectID
// the second returned value is true if the object was found,
// otherwise it's false
func (s *slabPool) get(obj ObjAddr) []byte {
	var res []byte
	sh := (*reflect.SliceHeader)(unsafe.Pointer(&res))
	sh.Data = obj
	sh.Len = int(s.objSize)
	sh.Cap = int(s.objSize)
	return res
}

func (s *slabPool) deleteSlab(slabId int) error {
	if slabId >= len(s.slabs) {
		return fmt.Errorf("Delete failed: Slab %d does not exist", slabId)
	}

	// unmap the data slice of the slab at slabId
	currentSlab := s.slabs[slabId]
	err := syscall.Munmap(currentSlab.data)
	if err != nil {
		return err
	}

	// delete the slab at slabId from the slice of slabs
	copy(s.slabs[slabId:], s.slabs[slabId+1:])
	s.slabs[len(s.slabs)-1] = slab{}
	s.slabs = s.slabs[:len(s.slabs)-1]

	return nil
}

// delete takes an objectID and deletes it from the slabPool
// on success it returns true, otherwise false
func (s *slabPool) delete(obj ObjAddr) error {
	slabId := s.findSlabByObjAddr(obj)
	if slabId < 0 || slabId >= len(s.slabs) {
		return fmt.Errorf("Delete failed: SlabID for object could not be found")
	}

	slab := s.slabs[slabId]
	objPos := uint(obj-slab.addr()) / uint(s.objSize)
	if objPos >= uint(s.objsPerSlab) {
		return fmt.Errorf("Delete failed: Could not calculate position of object")
	}
	slab.free.Clear(objPos)

	if slab.free.None() {
		err := s.deleteSlab(slabId)
		if err != nil {
			return err
		}
	}

	return nil
}
