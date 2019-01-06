package gos

import (
	"fmt"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestAddingGettingObjects3(t *testing.T) {
	testAddingGettingObjects(t, 3, 0, 1000)
}

func TestAddingGettingObjects12(t *testing.T) {
	testAddingGettingObjects(t, 50, 1000000000000, 1000000005000)
}

func testAddingGettingObjects(t *testing.T, objPerSlab, start, stop int) {
	os := NewObjectStore(uint(objPerSlab))

	testData := make(map[string]ObjAddr)
	for i := start; i < stop; i++ {
		testData[fmt.Sprintf("%d", i)] = 0
	}

	Convey("When adding test objects of varying sizes to the object store", t, func() {
		for obj := range testData {
			objAddr, err := os.Add([]byte(obj))
			So(err, ShouldBeNil)
			So(objAddr, ShouldBeGreaterThan, 0)
			testData[obj] = objAddr
		}

		Convey("then we should be able to look them up by object address", func() {
			for obj, addr := range testData {
				res, err := os.Get(addr)
				So(err, ShouldBeNil)
				So(string(res), ShouldEqual, obj)
			}
		})
	})
}

func TestAddingAndDeletingObjects(t *testing.T) {
	objectsPerSlab := uint(3)
	expectedSlabs := uint(3)
	os := NewObjectStore(objectsPerSlab)

	testData := make(map[string]ObjAddr)
	for i := uint(0); i < objectsPerSlab*expectedSlabs; i++ {
		testData[fmt.Sprintf("%05d", i)] = 0
	}

	Convey("When adding test objects to the object store", t, func() {
		for obj := range testData {
			objAddr, err := os.Add([]byte(obj))
			So(err, ShouldBeNil)
			So(objAddr, ShouldBeGreaterThan, 0)
			testData[obj] = objAddr
		}

		Convey("then we should be able to see the right number of slab", func() {
			So(len(os.slabPools[5].slabs), ShouldEqual, expectedSlabs)

			Convey("then we delete the objects again", func() {
				for _, obj := range testData {
					err := os.Delete(obj)
					So(err, ShouldBeNil)
				}

				Convey("now there should be no slabs anymore", func() {
					So(len(os.slabPools[5].slabs), ShouldEqual, 0)
				})
			})
		})
	})
}
