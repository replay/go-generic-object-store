package main

import (
	"bytes"
	"syscall"
)

var objectsPerSlab uint8 = 100

type slab struct {
	free freeList
	data []byte
}

type sizedPool struct {
	slabs   []slab
	objSize uint8
}

func (s *sizedPool) add(obj []byte) ([]byte, error) {
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

	var err error
	if !success {
		slabId, err = s.addSlab()
		if err != nil {
			return nil, err
		}
	}

	s.slabs[slabId].free.setUsed(pos)
	offset := int(pos) * int(s.objSize)
	for i := 0; i < int(s.objSize); i++ {
		s.slabs[slabId].data[i+offset] = obj[i]
	}

	return s.slabs[slabId].data[offset : offset+int(s.objSize)], nil
}

func (s *sizedPool) search(searching []byte) ([]byte, bool) {
	for _, slab := range s.slabs {
		for i := uint8(0); i < objectsPerSlab; i++ {
			offset := i * s.objSize
			obj := slab.data[offset : offset+s.objSize]
			if slab.free.isUsed(i) && bytes.Equal(searching, obj) {
				return obj, true
			}
		}
	}
	return nil, false
}

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
