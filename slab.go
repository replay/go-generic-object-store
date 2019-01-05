package gos

import (
	"reflect"
	"syscall"
	"unsafe"

	"github.com/willf/bitset"
)

type SlabAddr = uintptr

type slab struct {
	objSize     uint8
	objsPerSlab uint8
}

const sizeOfBitSet = unsafe.Sizeof(uint(0)) + unsafe.Sizeof([]byte{})
const sizeOfUint = unsafe.Sizeof(uint(0))

var bitSetDataLen int

func newSlab(objSize, objsPerSlab uint8) *slab {
	bitset.LittleEndian()
	bitSet := bitset.New(uint(objsPerSlab))
	bitSetDataLen = len(bitSet.Bytes())

	// 1 byte for the objSize, the BitSet struct, the BitSet data, the object slots (size * number)
	totalLen := 1 + int(sizeOfBitSet) + bitSetDataLen + int(objSize)*int(objsPerSlab)

	data, _ := syscall.Mmap(-1, 0, totalLen, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_ANON|syscall.MAP_PRIVATE)

	data[0] = byte(objSize)

	var copyFrom []byte
	copyFromHeader := (*reflect.SliceHeader)(unsafe.Pointer(&copyFrom))
	copyFromHeader.Data = uintptr(unsafe.Pointer(bitSet))
	copyFromHeader.Cap = int(sizeOfBitSet)
	copyFromHeader.Len = int(sizeOfBitSet)

	// copy the bitSet data structure into memory area at offset 1
	copy(data[1:], copyFrom)

	// get the byte slice header of bitset's data property
	bitSetDataSlice := (*reflect.SliceHeader)(unsafe.Pointer(&data[1+int(sizeOfUint)]))

	// set the data pointer to point at the address right after the bitSet struct
	bitSetDataSlice.Data = uintptr(unsafe.Pointer(&data[1+int(sizeOfBitSet)]))

	return (*slab)(unsafe.Pointer(&data[0]))
}

func (s *slab) addr() SlabAddr {
	return SlabAddr(unsafe.Pointer(&s))
}

func (s *slab) bitSet() *bitset.BitSet {
	return (*bitset.BitSet)(unsafe.Pointer(uintptr(unsafe.Pointer(s)) + 1))
}

func (s *slab) getObjsPerSlab() uint {
	return uint(s.objsPerSlab)
}
