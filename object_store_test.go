package gos

import (
	"fmt"
	"strconv"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestAddingGettingObjects3(t *testing.T) {
	testAddingGettingObjects(t, 3, 0, 1000)
}

func TestAddingGettingObjects13(t *testing.T) {
	testAddingGettingObjects(t, 50, 1000000000000, 1000000005000)
}

func testAddingGettingObjects(t *testing.T, objPerSlab, start, stop uint64) {
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

func TestAddingAndDeletingLargeNumberOfObjects(t *testing.T) {
	objectsPerSlab := uint(100)
	expectedSlabs := uint(100)
	objectSizes := []int{1, 4, 5, 50, 255}
	testData := make(map[string]ObjAddr)

	for i := uint(0); i < objectsPerSlab*expectedSlabs; i++ {
		testData[fmt.Sprintf("%0"+strconv.Itoa(objectSizes[i%uint(len(objectSizes))])+"d", i)] = 0
	}

	os := NewObjectStore(objectsPerSlab)

	Convey("When adding lots of test data to object store", t, func() {
		for k := range testData {
			objAddr, err := os.Add([]byte(k))
			So(err, ShouldBeNil)
			So(objAddr, ShouldBeGreaterThan, 0)
			testData[k] = objAddr
		}

		Convey("then we should be able to retrieve each of them", func() {
			for testValue, objAddr := range testData {
				retrievedValue, err := os.Get(objAddr)
				So(err, ShouldBeNil)
				So(string(retrievedValue), ShouldEqual, testValue)
			}

			Convey("then we can delete them again", func() {
				for _, objAddr := range testData {
					err := os.Delete(objAddr)
					So(err, ShouldBeNil)
				}

				Convey("after deleting everything there should be no slabs left anymore", func() {
					for _, slabPool := range os.slabPools {
						So(len(slabPool.slabs), ShouldBeZeroValue)
					}
				})
			})
		})
	})
}

func BenchmarkAddingDeleting(b *testing.B) {
	os := NewObjectStore(100)

	testData := make(map[string]ObjAddr)
	for i := 0; i < 99999; i++ {
		testData[fmt.Sprintf("%d", i)] = 0
	}

	b.ReportAllocs()
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		for testValue := range testData {
			testData[testValue], _ = os.Add([]byte(testValue))
		}
		for testValue := range testData {
			os.Delete(testData[testValue])
		}
	}
}

func BenchmarkSearchingForValue(b *testing.B) {

}
