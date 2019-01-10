package gos

import (
	"fmt"
	"reflect"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"unsafe"

	"github.com/willf/bitset"
)

// slabPool is a struct that contains and manages multiple slabs of data
// all objects in all the slabs must have the same size
type slabPool struct {
	slabs       []*slab
	objSize     uint8
	objsPerSlab uint
	freeSlabs   *bitset.BitSet
}

// NewSlabPool initializes a new slab pool and returns a pointer to it
func NewSlabPool(objSize uint8, objsPerSlab uint) *slabPool {
	return &slabPool{
		objSize:     objSize,
		objsPerSlab: objsPerSlab,
		freeSlabs:   bitset.New(0),
	}
}

// add adds an object to the pool
// It will try to find a slab that has a free object slot to avoid
// unnecessary allocations. If it can't find a free slot, it will add a
// slab and then use that one
// The first return value is the ObjAddr of the added object
// The second value is the slab address if the call created a new slab
// If no new slab has been created, then the second value is 0
// The third value is nil if there was no error, otherwise it is the error
func (s *slabPool) add(obj []byte) (ObjAddr, SlabAddr, error) {
	var success, full bool
	var objAddr ObjAddr
	var currentSlab *slab
	var idx int

	// find a slab where the addObj call succeeds
	// on full slabs the returned success value is false
	for idx, currentSlab = range s.slabs {
		objAddr, full, success = currentSlab.addObj(obj)
		if success {
			if full {
				// mark that slab as being full
				s.freeSlabs.Set(uint(idx))
			}
			// the object has been added
			return objAddr, 0, nil
		}
	}

	// the previous loop has not found a slab with free space,
	// so we add a new one
	slabIdx, err := s.addSlab()
	if err != nil {
		return 0, 0, err
	}

	currentSlab = s.slabs[slabIdx]

	// add the object to the new slab
	objAddr, full, success = currentSlab.addObj(obj)
	if !success {
		return 0, 0, fmt.Errorf("Add: Failed adding object to new slab")
	}

	if full {
		// mark that slab as being full
		s.freeSlabs.Set(uint(slabIdx))
	}

	// a new slab has been created, so its address is returned as
	// the second return value
	return objAddr, currentSlab.addr(), nil
}

// delete takes an ObjAddr and a SlabAddr, it will delete the according
// object from the slab at the given address and update all the related
// properties.
// On error it returns an error, otherwise nil
func (s *slabPool) delete(obj ObjAddr, slabAddr SlabAddr) error {
	empty := slabFromSlabAddr(slabAddr).delete(obj)

	if empty {
		return s.deleteSlab(slabAddr)
	} else {
		slabIdx := s.findSlabByAddr(slabAddr)
		s.freeSlabs.Clear(uint(slabIdx))
	}

	return nil
}

// findSlabByObjAddr takes an object address or slab address and then
// finds the slab where this object exists by looking it up from
// its slab list.
// It returns the slab index if the correct slab was found, otherwise
// the return value is the number of known slabs.
// For the lookup to succeed it relies on s.slabs to be sorted in descending order
func (s *slabPool) findSlabByAddr(obj uintptr) int {
	return sort.Search(len(s.slabs), func(i int) bool { return s.slabs[i].addr() <= obj })
}

// addSlab adds another slab to the pool and initalizes the related structs
// on success the first returned value is the index of the new slab
// on failure the second returned value is the error message
func (s *slabPool) addSlab() (int, error) {
	addedSlab, err := newSlab(s.objSize, s.objsPerSlab)
	if err != nil {
		return 0, err
	}

	newSlabAddr := addedSlab.addr()

	// find the right location to insert the new slab
	// note that s.slabs must remain sorted
	insertAt := sort.Search(len(s.slabs), func(i int) bool { return s.slabs[i].addr() < newSlabAddr })
	s.slabs = append(s.slabs, &slab{})
	copy(s.slabs[insertAt+1:], s.slabs[insertAt:])
	s.slabs[insertAt] = addedSlab
	s.addSlabIntoBitSet(uint(insertAt))

	return insertAt, nil
}

// addSlabIntoBitSet takes an index where a new slab has been added and then modifies
// the bitset's data accordingly to reflect that change
func (s *slabPool) addSlabIntoBitSet(idx uint) {
	s.freeSlabs = bitset.From(bitSetInsert(s.freeSlabs.Bytes(), s.freeSlabs.Len(), idx))

}

// bitSetInsert takes a slice of uint64, a setLength which indicates how many bits of
// the slice's data are used, and an index which indicates where a bit should be
// inserted. Then it shifts all the bits in the uint64 slice to the right by 1, starting
// from the given index position, and sets the index position to 0.
// f.e. 111 with insert index 2 would become 1101
func bitSetInsert(b []uint64, setLength, idx uint) []uint64 {
	insertAtElement := idx / 64

	// if length of BitSet is a multiple of uint64 we need to allocate more space first
	if setLength%64 == 0 {
		b = append(b, uint64(0))
	}

	var i uint
	for i = uint(len(b) - 1); i > insertAtElement; i-- {
		// all elements above the position where we want to insert can simply by shifted
		b[i] >>= 1

		// then we take the last bit of the previous element and set it as
		// the first bit of the current element
		b[i] |= (b[i-1] & 1) << 63
	}

	// generate a mask to extract the data that we need to shift right
	// within the element where we insert a bit
	dataMask := uint64(1)<<(uint64(64)-uint64(idx)%64) - 1

	// extract that data that we'll shift
	data := b[i] & dataMask

	// set the positions of the data mask to 0 in the element where we insert
	b[i] &= ^dataMask

	// shift data mask to the right and insert its data to the slice element
	b[i] |= data >> 1

	return b
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
	// to do so we need to built a byte slice that refers to the whole
	// slab as its underlying memory area
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

// search searches for a byte slice with the length of
// this slab's objectSize.
// When found it returns the object address and true,
// otherwise the second returned value is false
func (s *slabPool) search(searching []byte) (ObjAddr, bool) {
	wg := sync.WaitGroup{}
	objSize := int(s.objSize)
	var result uintptr

	goMaxProcs := runtime.GOMAXPROCS(0)
	wg.Add(goMaxProcs)
	for i := 0; i < goMaxProcs; i++ {
		go func(routineID int) {
			defer wg.Done()

			for slabID := routineID; slabID < len(s.slabs); slabID += goMaxProcs {
				currentSlab := s.slabs[slabID]

			OBJECT:
				for objID := uint(0); objID < s.objsPerSlab; objID++ {

					if currentSlab.bitSet().Test(objID) {
						obj := currentSlab.getObjByIdx(objID)
						for j := 0; j < objSize; j++ {
							if obj[j] != searching[j] {
								continue OBJECT
							}
						}

						// found it, store the result atomically
						atomic.StoreUintptr(&result, objAddrFromObj(obj))
						return
					}
				}

				// if result has been found by another thread we can exit this thread
				if atomic.LoadUintptr(&result) > 0 {
					return
				}
			}
		}(i)
	}

	wg.Wait()

	return result, result > 0
}

// searchBatched searches for a batch of search objects.
// It is similar to the search method, but it can do many searches at once.
// The returned value is a slice of ObjAddr which always has the same length
// as the slice of searched objects.
// If a searched object has been found then its address is at the same index
// in the returned slice as it was in the search slice.
// If a searched object has not been found, then the value in the returned
// slice is 0 at the index of the searched object.
func (s *slabPool) searchBatched(searching [][]byte) []ObjAddr {
	wg := sync.WaitGroup{}

	// preallocate the result set that will be returned
	resultSet := make([]ObjAddr, len(searching))
	resultsLeft := int32(len(searching))
	objSize := int(s.objSize)

	wg.Add(len(s.slabs))
	for i := range s.slabs {

		// every slab gets a go routine which searches for all searched objects
		go func(currentSlab *slab) {
			defer wg.Done()

			// iterate over objects in slab
			for j := uint(0); j < s.objsPerSlab; j++ {

				// if the current object slot is in use, then we compare its
				// value to the searched objects
				if currentSlab.bitSet().Test(j) {
					storedObj := currentSlab.getObjByIdx(j)

					// compare all searched objects to the stored object
				SEARCH:
					for k, searchedObj := range searching {
						for l := 0; l < objSize; l++ {
							if storedObj[l] != searchedObj[l] {
								continue SEARCH
							}

						}

						// found one search term, store it in the right location atomically
						atomic.StoreUintptr(&resultSet[k], objAddrFromObj(storedObj))

						// decrease number of searches left by one
						atomic.AddInt32(&resultsLeft, -1)
					}
				}

				if atomic.LoadInt32(&resultsLeft) == 0 {
					// all search terms have been found, exit routine
					return
				}
			}
		}(s.slabs[i])
	}

	wg.Wait()

	return resultSet
}

// get returns an object of the given object address as a byte slice
func (s *slabPool) get(obj ObjAddr) []byte {
	return objFromObjAddr(obj, s.objSize)
}
