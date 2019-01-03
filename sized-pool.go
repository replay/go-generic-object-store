
var objectsPerSlab = 100

type freeList uint64 // can keep track of up to 256 objects

type slab struct {
	free freeList
	data string // byte slice, but we want to save the capacity property because we know this won't grow
}

type sizedPool struct {
	slabs []slab
}

func (s *sizedPool) add(obj []byte) error {
	pos := int8(-1)
	var slabId int
	for slabId, slab := range s.slabs {
		pos = slab.freeList.getFree()
		if pos >= 0 {
			break
		}
	}
	if pos < 0 {
		s.addSlab()
	}
}