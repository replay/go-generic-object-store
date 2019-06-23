package gos

import (
	"crypto/md5"
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
	os := NewObjectStore(NewConfig())

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
	c := NewConfig()
	c.BaseObjectsPerSlab = objectsPerSlab
	c.GrowthExponent = 1
	os := NewObjectStore(c)

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
					pool, ok := os.slabPools[5]
					So(pool, ShouldBeNil)
					So(ok, ShouldBeFalse)
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

	c := NewConfig()
	c.BaseObjectsPerSlab = objectsPerSlab
	c.GrowthExponent = 1
	os := NewObjectStore(c)

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

func TestMemStats63Objects(t *testing.T) {
	objectsPerSlab := uint(63)
	objectSize := uint8(10)

	objects := [][]byte{
		[]byte("1234567890"),
	}

	c := NewConfig()
	c.BaseObjectsPerSlab = objectsPerSlab
	c.GrowthExponent = 1
	os := NewObjectStore(c)
	for _, o := range objects {
		os.Add(o)
	}

	Convey("When using less than 64 objects per slab", t, func() {
		memSize, err := os.MemStatsByObjSize(objectSize)
		So(err, ShouldBeNil)
		So(memSize, ShouldEqual, (1 + 32 + 8 + (10 * 63)))
	})
}

func TestMemStats65Objects(t *testing.T) {
	objectsPerSlab := uint(65)
	objectSize := uint8(10)

	objects := [][]byte{
		[]byte("1234567890"),
	}

	c := NewConfig()
	c.BaseObjectsPerSlab = objectsPerSlab
	c.GrowthExponent = 1
	os := NewObjectStore(c)
	for _, o := range objects {
		os.Add(o)
	}

	Convey("When using less than 64 objects per slab", t, func() {
		memSize, err := os.MemStatsByObjSize(objectSize)
		So(err, ShouldBeNil)
		So(memSize, ShouldEqual, (1 + 32 + 16 + (10 * 65)))
	})
}

func BenchmarkAddingDeleting(b *testing.B) {
	c := NewConfig()
	c.BaseObjectsPerSlab = 100
	c.GrowthExponent = 1
	os := NewObjectStore(c)

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
	testValueCount := 1000000
	testValues := make([][]byte, testValueCount)

	c := NewConfig()
	c.BaseObjectsPerSlab = 100
	c.GrowthExponent = 1
	os := NewObjectStore(c)

	for i := 0; i < testValueCount; i++ {
		testValues[i] = []byte(fmt.Sprintf("%x", md5.Sum([]byte(fmt.Sprintf("%d", i)))))
		os.Add(testValues[i])
	}

	b.ReportAllocs()
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		_, found := os.Search(testValues[n%testValueCount])
		if !found {
			b.Errorf("Value %d has not been found, but should have", n)
		}
	}
}
