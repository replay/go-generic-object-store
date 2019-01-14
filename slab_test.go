package gos

import (
	"fmt"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestNewSlab(t *testing.T) {
	newSlab(5, 10)
}

func TestSlabBitset(t *testing.T) {
	Convey("When creating a new slab", t, func() {
		objSize := uint8(5)
		objsPerSlab := uint(10000)
		slab, err := newSlab(objSize, objsPerSlab)
		So(err, ShouldBeNil)
		So(slab.objSize, ShouldEqual, objSize)
		So(slab.objsPerSlab(), ShouldEqual, objsPerSlab)
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

func TestSettingGettingObjects(t *testing.T) {
	Convey("When creating a new slab", t, func() {
		objSize := uint8(5)
		objsPerSlab := uint(100)
		slab, err := newSlab(objSize, objsPerSlab)
		So(err, ShouldBeNil)

		Convey("we should be able to set an object", func() {
			objValue := "abcde"
			objAddr, full, success := slab.addObj([]byte(objValue), 0)
			So(full, ShouldBeFalse)
			So(success, ShouldBeTrue)

			Convey("and access it via the object address", func() {
				So(string(objFromObjAddr(objAddr, objSize)), ShouldEqual, objValue)
			})
		})
	})
}

func TestSettingGettingManyObjects(t *testing.T) {
	Convey("When creating a new slab", t, func() {
		objSize := uint8(5)
		objsPerSlab := uint(100)
		slab, err := newSlab(objSize, objsPerSlab)
		var objAddresses []ObjAddr
		So(err, ShouldBeNil)

		Convey("we should be able to fill it up with objects", func() {
			for i := uint(0); i < objsPerSlab; i++ {
				value := fmt.Sprintf("%05d", i)
				objAddr, full, success := slab.addObj([]byte(value), i)
				if i == objsPerSlab-1 {
					So(full, ShouldBeTrue)
				} else {
					So(full, ShouldBeFalse)
				}
				So(success, ShouldBeTrue)
				objAddresses = append(objAddresses, objAddr)
			}

			Convey("and retreive all of them again", func() {
				for i := uint(0); i < objsPerSlab; i++ {
					obtainedValue := objFromObjAddr(objAddresses[i], objSize)
					So(string(obtainedValue), ShouldEqual, fmt.Sprintf("%05d", i))
				}
			})
		})
	})
}
