package gos

import (
	"fmt"
	"reflect"
	"testing"
	"unsafe"

	. "github.com/smartystreets/goconvey/convey"
)

func TestSlabFastCopy(t *testing.T) {
	os := NewObjectStore(100)

	addrs := make([]ObjAddr, 10)
	testData := make([][]byte, 10)
	testRes := make([][]byte, 10)
	longByte := []byte{1, 2, 3, 99, 99, 99}
	testData[0] = []byte{1, 2, 3}
	testData[1] = []byte{4, 5, 6}
	testData[2] = []byte{7, 8, 9}
	testData[3] = []byte{10, 11, 12}
	testData[4] = []byte{13, 14, 15}
	testData[5] = []byte{16, 17, 18}
	testData[6] = []byte{19, 20, 21}
	testData[7] = []byte{22, 23, 24}
	testData[8] = []byte{25, 26, 27}
	testData[9] = []byte{28, 29, 30}

	Convey("When adding test objects to the object store", t, func() {
		copyFromHeader := (*reflect.SliceHeader)(unsafe.Pointer(&longByte))
		copyFromHeader.Cap = int(3)
		copyFromHeader.Len = int(3)
		objAddr, _ := os.Add(longByte)
		addrs[0] = objAddr
		for i := 1; i < 10; i++ {
			objAddr, err := os.Add(testData[i])
			So(err, ShouldBeNil)
			So(objAddr, ShouldBeGreaterThan, 0)
			addrs[i] = objAddr
		}
		Convey("then retrieve the values and add them to a different slice", func() {
			for i := 1; i < 10; i++ {
				res, err := os.Get(addrs[i])
				So(err, ShouldBeNil)
				testRes[i] = res
				So(testData[i], ShouldResemble, testRes[i])
			}
		})
	})

}
func TestAddingGettingObjects(t *testing.T) {
	os := NewObjectStore(3)

	testData := make(map[string]ObjAddr)
	for i := 0; i < 1000; i++ {
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

			Convey("there should be 3 different slab pools", func() {
				So(len(os.slabPools), ShouldEqual, 3)
			})
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
