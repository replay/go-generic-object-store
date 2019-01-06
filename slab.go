package gos

import (
	"reflect"
	"syscall"
	"unsafe"

	"github.com/willf/bitset"
)

var offsetOfBitSetData = reflect.TypeOf(bitset.BitSet{}).Field(1).Offset

const sizeOfBitSet = unsafe.Sizeof(bitset.BitSet{})

type slab struct {
	objSize uint8
}

func newSlab(objSize uint8, objsPerSlab uint) (*slab, error) {
	bitSet := bitset.New(objsPerSlab)

	bitSetDataLen := len(bitSet.Bytes()) * 8
	sizeOfBitSet := unsafe.Sizeof(*bitSet)

	// 1 byte for the objSize, the BitSet struct, the BitSet data, the object slots (size * number)
	totalLen := 1 + int(sizeOfBitSet) + bitSetDataLen + int(objSize)*int(objsPerSlab)

	data, err := syscall.Mmap(-1, 0, totalLen, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_ANON|syscall.MAP_PRIVATE)
	if err != nil {
		return nil, err
	}

	// set the objSize property of the new slab
	data[0] = byte(objSize)

	// create temporary byte slice that accesses bitSet as underlying data
	// that way we can read the bitSet like a byte slice
	var copyFrom []byte
	copyFromHeader := (*reflect.SliceHeader)(unsafe.Pointer(&copyFrom))
	copyFromHeader.Data = uintptr(unsafe.Pointer(bitSet))
	copyFromHeader.Cap = int(sizeOfBitSet)
	copyFromHeader.Len = int(sizeOfBitSet)

	// copy the bitSet data structure into memory area at offset 1
	copy(data[1:], copyFrom)

	// get the byte slice header of bitset's data property
	bitSetDataSlice := (*reflect.SliceHeader)(unsafe.Pointer(&data[1+offsetOfBitSetData]))

	// set the data pointer to point at the address right after the bitSet struct
	bitSetDataSlice.Data = uintptr(unsafe.Pointer(&data[1+int(sizeOfBitSet)]))

	// return the data byte slice as a slab
	return (*slab)(unsafe.Pointer(&data[0])), nil
}

func (s *slab) addr() SlabAddr {
	return SlabAddr(unsafe.Pointer(s))
}

func (s *slab) bitSet() *bitset.BitSet {
	return (*bitset.BitSet)(unsafe.Pointer(uintptr(unsafe.Pointer(s)) + 1))
}

func (s *slab) objsPerSlab() uint {
	return s.bitSet().Len()
}

func (s *slab) getObjOffset(idx uint) uintptr {
	// offset where the object data begins
	dataOffset := uintptr(1) + sizeOfBitSet + uintptr(len(s.bitSet().Bytes())*8)

	// offset where the object is within the data range
	objectOffset := uintptr(s.objSize) * uintptr(idx)

	return dataOffset + objectOffset
}

func (s *slab) getObjIdx(obj ObjAddr) uint {
	// offset where the object data begins
	dataOffset := uintptr(1) + sizeOfBitSet + uintptr(len(s.bitSet().Bytes())*8)

	// offset where the object is within the data range
	objectOffset := obj - dataOffset - uintptr(unsafe.Pointer(s))

	// calculate index based on object offset and object size
	return uint(objectOffset / uintptr(s.objSize))
}

func (s *slab) addObjByIdx(idx uint, obj []byte) ObjAddr {
	offset := s.getObjOffset(idx)

	// objAddr will be the unique identifier of the newly created object
	objAddr := uintptr(unsafe.Pointer(s)) + offset

	p := unsafe.Pointer(objAddr)
	for i := 0; i < len(obj); i++ {
		*((*byte)(p)) = obj[i]
		p = unsafe.Pointer((uintptr(p)) + 1)
	}

	s.bitSet().Set(idx)

	return objAddr
}

func (s *slab) delete(obj ObjAddr) bool {
	idx := s.getObjIdx(obj)
	bitSet := s.bitSet()
	bitSet.Clear(idx)
	return bitSet.None()
}

func (s *slab) getObjByIdx(idx uint) []byte {
	offset := s.getObjOffset(idx)

	var res []byte
	resHeader := (*reflect.SliceHeader)(unsafe.Pointer(&res))
	resHeader.Data = uintptr(unsafe.Pointer(s)) + offset
	resHeader.Cap = 5
	resHeader.Len = 5

	return res
}
