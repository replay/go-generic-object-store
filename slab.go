package gos

import (
	"reflect"
	"syscall"
	"unsafe"

	"github.com/willf/bitset"
)

type SlabAddr = uintptr

type slab struct {
	objSize uint8
}

func newSlab(objSize uint8, objsPerSlab uint) *slab {
	offsetOfBitSetData := reflect.TypeOf(bitset.BitSet{}).Field(1).Offset
	bitSet := bitset.New(objsPerSlab)
	bitSetDataLen := len(bitSet.Bytes())
	sizeOfBitSet := unsafe.Sizeof(*bitSet)

	// 1 byte for the objSize, the BitSet struct, the BitSet data, the object slots (size * number)
	totalLen := 1 + int(sizeOfBitSet) + bitSetDataLen + int(objSize)*int(objsPerSlab)

	data, _ := syscall.Mmap(-1, 0, totalLen, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_ANON|syscall.MAP_PRIVATE)

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
	return (*slab)(unsafe.Pointer(&data[0]))
}

func (s *slab) addr() SlabAddr {
	return SlabAddr(unsafe.Pointer(&s))
}

func (s *slab) bitSet() *bitset.BitSet {
	return (*bitset.BitSet)(unsafe.Pointer(uintptr(unsafe.Pointer(s)) + 1))
}

func (s *slab) objsPerSlab() uint {
	return s.bitSet().Len()
}
