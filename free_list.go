package main

type freeList map[uint8]bool

func newFreeList() freeList {
	return make(freeList)
}

func (f freeList) setUsed(id uint8) {
	f[id] = true
}

func (f freeList) setUnused(id uint8) {
	f[id] = false
}

// getFree returns the position of the first free slot
// the second returned value indicates whether it found a free slot or not
func (f freeList) getFree() (uint8, bool) {
	for id, val := range f {
		if val {
			return id, true
		}
	}
	return 0, false
}
