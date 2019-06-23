package gos

import (
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"testing"
	"unsafe"

	. "github.com/smartystreets/goconvey/convey"
)

func TestAddingDeletingSlabs(t *testing.T) {
	objSize := uint8(10)
	sp := NewSlabPool(objSize)
	type objSlab struct {
		obj  ObjAddr
		slab SlabAddr
	}
	var objs []objSlab

	Convey("When adding objects to fill 3 slabs", t, func() {
		var currentSlab SlabAddr
		for i := 0; i < 3; i++ {
			value := fmt.Sprintf("%010d", i)
			objAddr, slabAddr, err := sp.add([]byte(value), 1, 1)
			So(err, ShouldBeNil)
			if slabAddr > 0 {
				currentSlab = slabAddr
			}
			objs = append(objs, objSlab{obj: objAddr, slab: currentSlab})
		}

		So(len(sp.slabs), ShouldEqual, 3)

		Convey("then we delete all objects again", func() {
			for _, objslab := range objs {
				_, err := sp.delete(objslab.obj, objslab.slab)
				So(err, ShouldBeNil)
			}

			So(len(sp.slabs), ShouldEqual, 0)
		})
	})
}

func TestAddingGettingManyObjects8(t *testing.T) {
	testAddingGettingManyObjects(t, 8, 10, 1.3)
}

func TestAddingGettingManyObjects10(t *testing.T) {
	testAddingGettingManyObjects(t, 10, 10, 100)
}

func TestAddingGettingManyObjects13(t *testing.T) {
	testAddingGettingManyObjects(t, 13, 2, 1)
}

func TestAddingGettingManyObjects15(t *testing.T) {
	testAddingGettingManyObjects(t, 15, 100, 2)
}

func TestAddingGettingManyObjects16(t *testing.T) {
	testAddingGettingManyObjects(t, 16, 2, 3)
}

func TestAddingGettingManyObjects17(t *testing.T) {
	testAddingGettingManyObjects(t, 17, 10, 4)
}

func testAddingGettingManyObjects(t *testing.T, objSize, baseObjCount uint8, growthFactor float64) {
	sp := NewSlabPool(objSize)
	objects := make(map[string]ObjAddr)

	Convey("When generating a set of many test objects", t, func() {
		var err error
		var i int
		// generate twice as many test object as there are objects per slab and add them to slabPool
		for ; i < int(baseObjCount)*75; i++ {
			value := fmt.Sprintf("%0"+strconv.Itoa(int(objSize))+"d", i)
			objects[value], _, err = sp.add([]byte(value), baseObjCount, growthFactor)
			So(err, ShouldBeNil)
		}

		Convey("We should be able to retreive each of them and get the correct value back", func() {
			var returned []byte
			for value, obj := range objects {
				returned = sp.get(obj)
				So(string(returned), ShouldEqual, value)
			}

			Convey("Then we delete all except one again", func() {
				skippedObject := ""
				for value, obj := range objects {
					if skippedObject == "" {
						skippedObject = value
						continue
					}
					sp.delete(obj, uintptr(unsafe.Pointer(sp.slabs[sp.findSlabByAddr(obj)])))
				}

				So(len(sp.slabs), ShouldEqual, 1)

				Convey("When we then re-add all the deleted ones again", func() {
					for value := range objects {
						if value == skippedObject {
							continue
						}
						sp.add([]byte(value), baseObjCount, growthFactor)
					}
				})
			})
		})
	})
}

func TestAddingSearchingObject(t *testing.T) {
	objSize := uint8(5)
	sp := NewSlabPool(objSize)
	testString1 := "abcde"
	testString2 := "aaaaa"
	var objAddr1, objAddr2 ObjAddr
	var success bool
	Convey("When adding a byte slice to the pool", t, func() {
		sp.add([]byte(testString1), 1, 1)

		Convey("we should be able to find it with the search method", func() {
			objAddr1, success = sp.search([]byte(testString1))
			So(success, ShouldBeTrue)
			result1 := sp.get(objAddr1)
			So(string(result1), ShouldEqual, testString1)
		})
	})
	Convey("When adding a second object", t, func() {
		sp.add([]byte(testString2), 1, 1)

		Convey("we should also be able to find it", func() {
			objAddr2, success = sp.search([]byte(testString2))
			So(success, ShouldBeTrue)
			result2 := sp.get(objAddr2)
			So(string(result2), ShouldEqual, testString2)
		})
	})
}

func TestAddingSearchingObjectInManySlabs(t *testing.T) {
	objSize := uint8(5)
	objsPerSlab := uint(10)
	expectedSlabs := uint(100)
	sp := NewSlabPool(objSize)
	Convey(fmt.Sprintf("When adding %d objects to the pool", objsPerSlab*expectedSlabs), t, func() {
		for i := uint(0); i < expectedSlabs*objsPerSlab; i++ {
			objAddr, _, err := sp.add([]byte(fmt.Sprintf("%05d", i)), uint8(objsPerSlab), 1)
			So(err, ShouldBeNil)
			So(objAddr, ShouldBeGreaterThan, 0)
		}

		Convey("we should be able to find any added object with the search method", func() {
			searchObjects := []string{"00325", "00999", "00000", "00010"}

			for _, searchObject := range searchObjects {
				addr, success := sp.search([]byte(searchObject))
				So(success, ShouldBeTrue)
				obj := sp.get(addr)
				So(string(obj), ShouldEqual, searchObject)
			}

			Convey("we should not be able to find an object that doesn't exist", func() {
				_, success := sp.search([]byte("abcde"))
				So(success, ShouldBeFalse)
			})
		})
	})
}

func TestBatchSearchingObjects(t *testing.T) {
	objSize := uint8(5)
	objsPerSlab := uint(10)
	expectedSlabs := uint(100)
	sp := NewSlabPool(objSize)

	Convey(fmt.Sprintf("When adding %d objects to the pool", objsPerSlab*expectedSlabs), t, func() {
		for i := uint(0); i < objsPerSlab*expectedSlabs; i++ {
			sp.add([]byte(fmt.Sprintf("%05d", i)), uint8(objsPerSlab), 1)
		}

		Convey("we should be able to search for them", func() {
			searchTerms := [][]byte{
				[]byte("00100"),
				[]byte("00320"),
				[]byte("ccccc"),
				[]byte("00999"),
				[]byte("00998"),
				[]byte("abcde"),
				[]byte("00000"),
				[]byte("00345"),
			}
			searchResults := sp.searchBatched(searchTerms)

			// these two search terms should not have been found
			So(searchResults[2], ShouldEqual, 0)
			So(searchResults[5], ShouldEqual, 0)

			So(string(objFromObjAddr(searchResults[0], 5)), ShouldEqual, string(searchTerms[0]))
			So(string(objFromObjAddr(searchResults[1], 5)), ShouldEqual, string(searchTerms[1]))
			So(string(objFromObjAddr(searchResults[3], 5)), ShouldEqual, string(searchTerms[3]))
			So(string(objFromObjAddr(searchResults[4], 5)), ShouldEqual, string(searchTerms[4]))
			So(string(objFromObjAddr(searchResults[6], 5)), ShouldEqual, string(searchTerms[6]))
			So(string(objFromObjAddr(searchResults[7], 5)), ShouldEqual, string(searchTerms[7]))
		})
	})
}

func TestFragmentedSlabPoolSizes(t *testing.T) {
	Convey("When adding 63 object with base objs per slab 1 and growth factor 2", t, func() {
		// config for objCounts per slab: 1, 2, 4, 8, 16
		baseObjsPerSlab := uint8(1)
		growthFactor := float64(2)

		pool := NewSlabPool(10)
		slabID := 0
		var i int
		var objAddrs []ObjAddr

		// generate 6 slabs (2^6-1 objects)
		for i = 0; i < 63; i++ {
			objAddr, slabAddr, err := pool.add([]byte(fmt.Sprintf("%10d", i)), baseObjsPerSlab, growthFactor)
			So(err, ShouldBeNil)
			if i == int(math.Pow(float64(2), float64(slabID)))-1 {
				So(slabAddr, ShouldNotBeNil)
				slabID++
			} else {
				So(slabAddr, ShouldBeZeroValue)
			}

			objAddrs = append(objAddrs, objAddr)
		}

		So(len(pool.slabs), ShouldEqual, 6)

		Convey("When delete all objects except the last one again", func() {
			slabID = 0
			i = 0
			for i = 0; i < 62; i++ {
				empty, err := pool.delete(objAddrs[i], uintptr(unsafe.Pointer(pool.slabs[pool.findSlabByAddr(objAddrs[i])])))
				So(err, ShouldBeNil)
				if i == int(math.Pow(float64(2), float64(slabID+1)))-2 {
					So(empty, ShouldBeTrue)
					slabID++
				} else {
					So(empty, ShouldBeFalse)
				}
			}

			So(len(pool.slabs), ShouldEqual, 1)

			Convey("When refilling the existing slab, no new ones should be created", func() {
				i = 0
				for i = 0; i < int(pool.slabs[0].objCount()-1); i++ {
					_, slabAddr, err := pool.add([]byte(fmt.Sprintf("%10d", i)), baseObjsPerSlab, growthFactor)
					So(err, ShouldBeNil)
					So(slabAddr, ShouldBeZeroValue)
				}

				// last slab has 2^5 objects, so 2^5-1 = 31
				So(i, ShouldEqual, 31)

				So(len(pool.slabs), ShouldEqual, 1)

				Convey("When adding one more object, a new slab with the size of the 2nd slab should get created", func() {
					objAddr, slabAddr, err := pool.add([]byte(fmt.Sprintf("%10d", 0)), baseObjsPerSlab, growthFactor)
					So(err, ShouldBeNil)
					So(objAddr, ShouldNotEqual, 0)
					So(slabAddr, ShouldNotEqual, 0)
					So(len(pool.slabs), ShouldEqual, 2)

					// with baseObjsPerSlab = 1 & growthFactor = 2 the second slab should have the size of 2 objects
					So(pool.slabs[pool.findSlabByAddr(objAddr)].objCount(), ShouldEqual, 2)
				})
			})
		})
	})
}

func BenchmarkAddingSearchingObjectInLargePool(b *testing.B) {
	objSize := uint8(20)
	objsPerSlab := uint(100)
	sp := NewSlabPool(objSize)
	type valueAndAddr struct {
		value []byte
		addr  ObjAddr
	}
	valueCount := 100000
	testValues := make([]valueAndAddr, valueCount)
	for i := 0; i < valueCount; i++ {
		value := []byte(fmt.Sprintf("%20d", i+valueCount))
		addr, _, err := sp.add([]byte(value), uint8(objsPerSlab), 1)
		if err != nil {
			b.Fatalf("Got error on add: %s", err)
		}
		testValues[i].value = value
		testValues[i].addr = addr
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		searchFor := testValues[rand.Int31n(int32(valueCount))]
		gotAddr, found := sp.search(searchFor.value)
		if !found || gotAddr != searchFor.addr {
			b.Fatalf(fmt.Sprintf("Got unexpected result:\nfound: %t\ngot addr: %d\nfound Addr: %d", found, gotAddr, searchFor.addr))
		}
	}
}

func BenchmarkAddingObjectsGrowthFactor1_3(b *testing.B) {
	sp := NewSlabPool(10)

	objs := make([][]byte, b.N)
	for i := 0; i < b.N; i++ {
		objs[i] = []byte(fmt.Sprintf("%10d", i))
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := range objs {
		sp.add(objs[i], 10, 1.3)
	}
}
func BenchmarkAddingObjectsGrowthFactor2(b *testing.B) {
	sp := NewSlabPool(10)

	objs := make([][]byte, b.N)
	for i := 0; i < b.N; i++ {
		objs[i] = []byte(fmt.Sprintf("%10d", i))
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := range objs {
		sp.add(objs[i], 10, 2)
	}
}
func BenchmarkAddingObjectsGrowthFactor4(b *testing.B) {
	sp := NewSlabPool(10)

	objs := make([][]byte, b.N)
	for i := 0; i < b.N; i++ {
		objs[i] = []byte(fmt.Sprintf("%10d", i))
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := range objs {
		sp.add(objs[i], 10, 2)
	}
}

func BenchmarkDeletingObjectsAndSlabs(b *testing.B) {
	sp := NewSlabPool(10)

	type result struct {
		obj  ObjAddr
		slab SlabAddr
	}
	results := make([]result, b.N)
	for i := 0; i < b.N; i++ {
		results[i].obj, results[i].slab, _ = sp.add([]byte(fmt.Sprintf("%10d", i)), 1, 1)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := range results {
		sp.delete(results[i].obj, results[i].slab)
	}
}

// func BenchmarkAddingSearchingObjectInLargePoolWithDeleteAndReinsert(b *testing.B) {
// 	objSize := uint8(20)
// 	objsPerSlab := uint(100)
// 	sp := NewSlabPool(objSize, objsPerSlab)
// 	type valueAndAddr struct {
// 		value    []byte
// 		objAddr  ObjAddr
// 		slabAddr SlabAddr
// 	}
// 	valueCount := 100000
// 	testValues := make([]valueAndAddr, valueCount)
// 	var lastSlabAddr SlabAddr
// 	for i := 0; i < valueCount; i++ {
// 		value := []byte(fmt.Sprintf("%20d", i+valueCount))
// 		objAddr, slabAddr, err := sp.add([]byte(value))
// 		if err != nil {
// 			b.Fatalf("Got error on add: %s", err)
// 		}
// 		if slabAddr > 0 {
// 			lastSlabAddr = slabAddr
// 		}
// 		testValues[i].value = value
// 		testValues[i].objAddr = objAddr
// 		testValues[i].slabAddr = lastSlabAddr
// 	}

// 	var err error
// 	deletedValues := make([]string, 10)
// 	for i := 0; i < valueCount/10; i++ {
// 		for j := 0; j < 10; j++ {
// 			deleteId := i*10 + j
// 			toDelete := testValues[deleteId]
// 			err = sp.delete(toDelete.objAddr, toDelete.slabAddr)
// 		}
// 	}

// 	b.ReportAllocs()
// 	b.ResetTimer()

// 	for i := 0; i < b.N; i++ {
// 		searchFor := testValues[rand.Int31n(int32(valueCount))]
// 		gotAddr, found := sp.search(searchFor.value)
// 		if !found || gotAddr != searchFor.addr {
// 			b.Fatalf(fmt.Sprintf("Got unexpected result:\nfound: %t\ngot addr: %d\nfound Addr: %d", found, gotAddr, searchFor.addr))
// 		}
// 	}
// }
