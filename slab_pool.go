package gos

import (
	"context"
	"encoding/binary"
	"fmt"
	"reflect"
	"runtime"
	"sort"
	"sync/atomic"
	"syscall"
	"unsafe"

	jump "github.com/dgryski/go-jump"
	"github.com/willf/bitset"
	"golang.org/x/sync/errgroup"
)

const hashRange = 10

// slabPool is a struct that contains and manages multiple slabs of data
// all objects in all the slabs must have the same size
type slabPool struct {
	slabs       []*slab
	objSize     uint8
	objsPerSlab uint
	freeSlabs   bitset.BitSet
}

// NewSlabPool initializes a new slab pool and returns a pointer to it
func NewSlabPool(objSize uint8, objsPerSlab uint) *slabPool {
	return &slabPool{
		objSize:     objSize,
		objsPerSlab: objsPerSlab,
		freeSlabs:   *bitset.New(0),
	}
}

func (s *slabPool) getNextSlabID(current, slabCount uint) uint {
	// in case of a small slab count there is no need to pay attention to the hash
	// we can simply iterate over all available slabs
	if slabCount >= hashRange {
		return (current + 1) % slabCount
	}

	if slabCount <= hashRange {
		return (current + 1) % slabCount
	}

	current += hashRange
	if current >= slabCount {
		current = (current + 1) % hashRange
	}

	return current
}

// getNextID generates the next ID to check for according to given parameters
// those IDs are used to find slabs in slices and objects in slabs
// current is the last used ID
// objHash is a hash of the object we find an ID for, it must be > 0 and < max
// max is the max value that we can accept as ID, exclusive
func (s *slabPool) getNextID(current, objHash, max uint) uint {
	var next uint

	if objHash > max {
		objHash = objHash % max
	}

	objHash++

	next = current + objHash
	if next >= max {
		next = next % objHash
		if next == 0 {
			next = objHash - 1
		} else {
			next--
		}
	}

	return next
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
	var currentSlab *slab
	var objHash uint
	var hashInput []byte

	if len(obj) < 8 {
		hashInput = append(make([]byte, 8-len(obj)), obj...)
	} else {
		hashInput = obj[len(obj)-8:]
	}

	slabCount := uint(len(s.slabs))
	objHash = uint(jump.Hash(binary.LittleEndian.Uint64(hashInput), hashRange))
	objIdx := objHash
	slabIdx := objHash

	found := false
	if slabCount > 0 {
		for i := uint(0); i < slabCount; i++ {
			slabIdx = s.getNextSlabID(slabIdx, slabCount)
			if !s.freeSlabs.Test(slabIdx) {
				slabBitSet := s.slabs[slabIdx].bitSet()
				objIdx, found = slabBitSet.NextClear(objIdx)
				if !found {
					objIdx, found = slabBitSet.NextClear(0)
				}

				if found {
					currentSlab = s.slabs[slabIdx]
					break
				}
			}
		}
	}

	var newSlab SlabAddr
	if !found {
		newIdx, err := s.addSlab()
		if err != nil {
			return 0, 0, err
		}
		currentSlab = s.slabs[newIdx]
		slabIdx = uint(newIdx)
		newSlab = SlabAddr(unsafe.Pointer(currentSlab))
		objIdx = objHash
	}

	objAddr, full, success := currentSlab.addObj(obj, objIdx)
	if !success {
		// this shouldn't happen, because we first checked via freeSlabs
		// whether this slab has space or not
		return 0, 0, fmt.Errorf("Add: Failed to add object into slab")
	}
	if full {
		// mark that slab as full so nothing more gets added
		s.freeSlabs.Set(slabIdx)
	}

	return objAddr, newSlab, nil
}

// delete takes an ObjAddr and a SlabAddr, it will delete the according
// object from the slab at the given address and update all the related
// properties.
// On success it returns true and nil if the slab was also deleted.
// On success it returns false and nil if the slab was not also deleted.
// On error it returns false and an error.
func (s *slabPool) delete(obj ObjAddr, slabAddr SlabAddr) (bool, error) {
	empty := slabFromSlabAddr(slabAddr).delete(obj)

	if empty {
		return s.deleteSlab(slabAddr)
	} else {
		// the slab isn't empty, but since we've just deleted an object
		// we know that there is at least one free slot, so we mark it
		// accordingly
		slabIdx := s.findSlabByAddr(slabAddr)
		s.freeSlabs.Clear(uint(slabIdx))
	}

	return false, nil
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

	s.freeSlabs.InsertAt(uint(insertAt))

	return insertAt, nil
}

// deleteSlab deletes the slab at the given slab index
// on failure it returns false and an error
// on success it returns true and nil
func (s *slabPool) deleteSlab(slabAddr SlabAddr) (bool, error) {
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
		return false, err
	}

	s.freeSlabs.DeleteAt(uint(slabIdx))

	return true, nil
}

// search searches for a byte slice with the length of
// this slab's objectSize.
// When found it returns the object addresses in a slice, the slice offsets
// of the search results are consistent with the offsets of the search terms
func (s *slabPool) searchBatched(searches [][]byte) []ObjAddr {
	type searchTarget struct {
		searchTerm int
		slabIdx    uint
	}

	type searchResult struct {
		searchTerm int
		objAddr    ObjAddr
	}

	// the number of procs we want to run is either the value of GOMAXPROCS or the
	// number of slabs present, whatever is smaller
	procs := runtime.GOMAXPROCS(0)
	if procs > len(s.slabs) {
		procs = len(s.slabs)
	}

	searchesLeft := int32(len(searches))
	slabCount := uint(len(s.slabs))
	searchTargets := make(chan searchTarget, procs)
	searchResults := make(chan searchResult, len(searches))
	foundTargets := make([]uint32, len(searches))

	currentSearchPositions := make([]uint, len(searches))
	var hashInput []byte
	for i, searchTerm := range searches {
		if len(searchTerm) < 8 {
			hashInput = append(make([]byte, 8-len(searchTerm)), searchTerm...)
		} else {
			hashInput = searchTerm[len(searchTerm)-8:]
		}
		hash := uint(jump.Hash(binary.LittleEndian.Uint64(hashInput), hashRange))
		currentSearchPositions[i] = hash
	}

	rootCtx, rootCancel := context.WithCancel(context.Background())
	g, ctx := errgroup.WithContext(rootCtx)
	g.Go(func() error {
		defer close(searchTargets)

		searchesPerTarget := make([]int, len(searches))
		for i := range searches {
			for j := uint(0); j < slabCount; j++ {
				if atomic.LoadUint32(&foundTargets[i]) > 0 {
					continue
				}

				currentSearchPositions[i] = s.getNextSlabID(currentSearchPositions[i], slabCount)

				target := searchTarget{
					searchTerm: i,
					slabIdx:    currentSearchPositions[i],
				}
				searchesPerTarget[i]++

				select {
				case <-ctx.Done():
					return nil
				case searchTargets <- target:
				}
			}
		}

		return nil
	})

	for i := 0; i < procs; i++ {
		g.Go(func() error {
		SEARCH_TARGETS:
			for target := range searchTargets {
				if atomic.LoadInt32(&searchesLeft) < 1 {
					rootCancel()
					return nil
				}

				slab := s.slabs[target.slabIdx]
				searchTerm := searches[target.searchTerm]

			OBJECTS:
				for objIdx := uint(0); objIdx < s.objsPerSlab; objIdx++ {
					if slab.bitSet().Test(objIdx) {
						obj := slab.getObjByIdx(objIdx)
						for j := 0; j < len(obj); j++ {
							if obj[j] != searchTerm[j] {
								continue OBJECTS
							}
						}

						// found the term
						atomic.StoreUint32(&foundTargets[target.searchTerm], 1)
						atomic.AddInt32(&searchesLeft, -1)
						searchResults <- searchResult{
							searchTerm: target.searchTerm,
							objAddr:    objAddrFromObj(obj),
						}

						continue SEARCH_TARGETS
					}
				}
			}

			return nil
		})
	}

	go func() {
		g.Wait()
		rootCancel()
		close(searchResults)
	}()

	returnValue := make([]ObjAddr, len(searches))
	for result := range searchResults {
		fmt.Println(fmt.Sprintf("setting result %+v", result))
		returnValue[result.searchTerm] = result.objAddr
	}

	return returnValue

	// objSize := int(s.objSize)

	// slabIndexes := make(chan uint)
	// g, ctx := errgroup.WithContext(nil)
	// g.Go(func() error {
	// 	defer close(slabIndexes)
	// 	var hashInput []byte
	// 	var objHash uint
	// 	if len(searches) < 8 {
	// 		hashInput = append(make([]byte, 8-len(searches)), searches...)
	// 	} else {
	// 		hashInput = searches[len(searches)-8:]
	// 	}
	// 	objHash = uint(jump.Hash(binary.LittleEndian.Uint64(hashInput), int(s.objsPerSlab)-1))
	// 	objHash++ // objHash must be >0

	// 	var slabIdx uint
	// 	for i := 0; i < len(s.slabs); i++ {
	// 		slabIdx = s.getNextSlabID(slabIdx, objHash)
	// 		slabIndexes <- slabIdx
	// 	}

	// 	return nil
	// })

	// searchesLeft := int32(len(searches))
	// results := make([]ObjAddr, len(searches))
	// for i := 0; i < procs; i++ {
	// 	g.Go(func() error {
	// 		for slabIdx := range slabIndexes {
	// 			if atomic.LoadInt32(&searchesLeft) <= 0 {
	// 				return nil
	// 			}

	// 			currentSlab := s.slabs[slabIdx]
	// 			for objID := uint(0); objID < s.objsPerSlab; objID++ {
	// 				if currentSlab.bitSet().Test(objID) {
	// 					obj := currentSlab.getObjByIdx(objID)

	// 				SEARCH:
	// 					for j, search := range searches {
	// 						if atomic.LoadUintptr(&results[j]) != 0 {
	// 							continue SEARCH
	// 						}

	// 						for k := 0; k < objSize; k++ {
	// 							if obj[k] != search[k] {
	// 								continue SEARCH
	// 							}
	// 						}

	// 						atomic.StoreUintptr(&results[j], ObjAddr(objID))
	// 						if atomic.AddInt32(&searchesLeft, -1) == 0 {
	// 							return nil
	// 						}
	// 					}

	// 					select {
	// 					case <-ctx.Done():
	// 						return ctx.Err()
	// 					}
	// 				}
	// 			}

	// 			select {
	// 			case <-ctx.Done():
	// 				return ctx.Err()
	// 			}
	// 		}

	// 		return nil
	// 	})
	// }

	// go func() {
	// 	g.Wait()
	// 	close(results)
	// }()

}

// searchBatched searches for a batch of search objects.
// It is similar to the search method, but it can do many searches at once.
// The returned value is a slice of ObjAddr which always has the same length
// as the slice of searched objects.
// If a searched object has been found then its address is at the same index
// in the returned slice as it was in the search slice.
// If a searched object has not been found, then the value in the returned
// slice is 0 at the index of the searched object.
func (s *slabPool) search(searching []byte) (ObjAddr, bool) {
	/*wg := sync.WaitGroup{}

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

	return resultSet*/
	return 0, false
}

// get returns an object of the given object address as a byte slice
func (s *slabPool) get(obj ObjAddr) []byte {
	return objFromObjAddr(obj, s.objSize)
}
