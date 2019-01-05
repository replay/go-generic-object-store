package gos

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestNewSlab(t *testing.T) {
	newSlab(5, 10)
}

func TestSlabBitset(t *testing.T) {
	Convey("When creating a new slab", t, func() {
		objSize := uint8(5)
		objsPerSlab := uint8(100)
		slab := newSlab(objSize, objsPerSlab)
		So(slab.objSize, ShouldEqual, objSize)
		So(slab.objsPerSlab, ShouldEqual, objsPerSlab)
		Convey("we should be able to obtain and use the bitset from it", func() {
			bitSet1 := slab.bitSet()
			bitSet1.Set(2)
			bitSet1.Set(4)
			bitSet1.Set(9)
			Convey("then we can obtain it a second time", func() {
				bitSet2 := slab.bitSet()
				So(bitSet2.Test(0), ShouldBeFalse)
				So(bitSet2.Test(1), ShouldBeFalse)
				So(bitSet2.Test(2), ShouldBeTrue)
				So(bitSet2.Test(3), ShouldBeFalse)
				So(bitSet2.Test(4), ShouldBeTrue)
				So(bitSet2.Test(5), ShouldBeFalse)
				So(bitSet2.Test(6), ShouldBeFalse)
				So(bitSet2.Test(7), ShouldBeFalse)
				So(bitSet2.Test(8), ShouldBeFalse)
				So(bitSet2.Test(9), ShouldBeTrue)
			})
		})
	})
}
