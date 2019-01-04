package main

type freeList map[uint8]bool

func newFreeList(size uint8) freeList {
	f := make(freeList)
	for i := uint8(0); i < size; i++ {
		f[i] = false
	}
	return f
}

func (f freeList) setUsed(id uint8) {
	f[id] = true
}

func (f freeList) setFree(id uint8) {
	f[id] = false
}

func (f freeList) isUsed(id uint8) bool {
	return f[id]
}

// getFree returns the position of the first free slot
// the second returned value indicates whether it found a free slot or not
func (f freeList) getFree() (uint8, bool) {
	for id, val := range f {
		if !val {
			return id, true
		}
	}
	return 0, false
}
