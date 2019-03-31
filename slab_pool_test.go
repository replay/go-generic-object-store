package gos

import (
	"fmt"
	"math/rand"
	"strconv"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestAddingDeletingSlabs(t *testing.T) {
	objSize := uint8(10)
	objsPerSlab := uint(1)
	sp := NewSlabPool(objSize, objsPerSlab)
	type objSlab struct {
		obj  ObjAddr
		slab SlabAddr
	}
	var objs []objSlab

	Convey("When adding 3 times as many objects as there are objects per slab", t, func() {
		var currentSlab SlabAddr
		for i := 0; i < int(objsPerSlab)*3; i++ {
			value := fmt.Sprintf("%010d", i)
			objAddr, slabAddr, err := sp.add([]byte(value))
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

func TestGettingNextIdForSearch(t *testing.T) {
	type testCase struct {
		expectedIDs []uint
		max         uint
		objHash     uint
	}

	testCases := []testCase{
		testCase{
			expectedIDs: []uint{1, 0, 1, 0, 1},
			max:         1,
			objHash:     1,
		},
		testCase{
			expectedIDs: []uint{7, 2, 6, 1, 5, 9, 0, 4, 8, 3, 7, 2},
			max:         10,
			objHash:     3,
		},
		testCase{
			expectedIDs: []uint{1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2},
			max:         10,
			objHash:     0,
		},
		testCase{
			expectedIDs: []uint{0, 1, 0, 1, 0, 1, 0},
			max:         2,
			objHash:     1,
		},
		testCase{
			expectedIDs: []uint{1, 0, 1, 0, 1},
			max:         2,
			objHash:     10,
		},
		testCase{
			expectedIDs: []uint{0, 1, 0, 1, 0},
			max:         2,
			objHash:     5,
		},
		testCase{
			expectedIDs: []uint{1, 0, 2, 1, 0, 2, 1, 0, 2},
			max:         3,
			objHash:     17,
		},
		testCase{
			expectedIDs: []uint{1, 0, 3, 2, 1, 0, 3, 2},
			max:         4,
			objHash:     2,
		},
		testCase{
			expectedIDs: []uint{0, 2, 1, 0, 2, 1},
			max:         3,
			objHash:     7,
		},
	}

	for tcIdx, tc := range testCases {
		sp := NewSlabPool(10, 10)

		current := tc.objHash % tc.max
		for i := uint(0); i < uint(len(tc.expectedIDs)); i++ {
			current = sp.getNextID(current, tc.objHash, tc.max)
			if current != tc.expectedIDs[i] {
				t.Fatalf("tc %d: Expected ID to be %d but it was %d", tcIdx, tc.expectedIDs[i], current)
			}
		}
	}
}

func TestAddingGettingManyObjects8(t *testing.T) {
	testAddingGettingManyObjects(t, 8, 10)
}

func TestAddingGettingManyObjects10(t *testing.T) {
	testAddingGettingManyObjects(t, 10, 10)
}

func TestAddingGettingManyObjects13(t *testing.T) {
	testAddingGettingManyObjects(t, 13, 10)
}

func TestAddingGettingManyObjects15(t *testing.T) {
	testAddingGettingManyObjects(t, 15, 10)
}

func TestAddingGettingManyObjects16(t *testing.T) {
	testAddingGettingManyObjects(t, 16, 10)
}

func TestAddingGettingManyObjects17(t *testing.T) {
	testAddingGettingManyObjects(t, 17, 10)
}

func testAddingGettingManyObjects(t *testing.T, objSz, objsPer int) {
	objSize := uint8(objSz)
	objsPerSlab := uint(objsPer)
	sp := NewSlabPool(objSize, objsPerSlab)
	objects := make(map[string]ObjAddr)

	Convey("When generating a set of many test objects", t, func() {
		var err error
		// generate twice as many test object as there are objects per slab and add them to slabPool
		for i := 0; i < int(objsPerSlab*2); i++ {
			value := fmt.Sprintf("%0"+strconv.Itoa(int(objSize))+"d", i)
			objects[value], _, err = sp.add([]byte(value))
			So(err, ShouldBeNil)
		}

		Convey("We should be able to retreive each of them and get the correct value back", func() {
			var returned []byte
			for i := 0; i < len(sp.slabs); i++ {
				fmt.Println(sp.slabs[i].String())
			}
			for value, obj := range objects {
				returned = sp.get(obj)
				So(string(returned), ShouldEqual, value)
			}
			So(len(sp.slabs), ShouldEqual, 2)
		})
	})
}

func TestAddingSearchingObject(t *testing.T) {
	objSize := uint8(5)
	objsPerSlab := uint(1)
	testString1 := "abcde"
	testString2 := "aaaaa"
	var objAddr1, objAddr2 ObjAddr
	var success bool
	Convey("After initializing the slap pool", t, func() {
		sp := NewSlabPool(objSize, objsPerSlab)

		Convey("When adding a byte slice to the pool", func() {
			sp.add([]byte(testString1))

			Convey("we should be able to find it with the search method", func() {
				objAddr1, success = sp.search([]byte(testString1))
				So(success, ShouldBeTrue)
				result1 := sp.get(objAddr1)
				So(string(result1), ShouldEqual, testString1)
			})
		})

		Convey("When adding a second object", func() {
			sp.add([]byte(testString2))

			Convey("we should also be able to find it", func() {
				objAddr2, success = sp.search([]byte(testString2))
				So(success, ShouldBeTrue)
				result2 := sp.get(objAddr2)
				So(string(result2), ShouldEqual, testString2)
			})
		})
	})
}

func TestAddingSearchingObjectInManySlabs(t *testing.T) {
	objSize := uint8(5)
	objsPerSlab := uint(10)
	expectedSlabs := uint(100)
	Convey(fmt.Sprintf("When adding %d objects to the pool", objsPerSlab*expectedSlabs), t, func() {
		sp := NewSlabPool(objSize, objsPerSlab)
		for i := uint(0); i < expectedSlabs*objsPerSlab; i++ {
			objAddr, _, err := sp.add([]byte(fmt.Sprintf("%05d", i)))
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
	sp := NewSlabPool(objSize, objsPerSlab)

	Convey(fmt.Sprintf("When adding %d objects to the pool", objsPerSlab*expectedSlabs), t, func() {
		for i := uint(0); i < objsPerSlab*expectedSlabs; i++ {
			sp.add([]byte(fmt.Sprintf("%05d", i)))
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

func TestSearchBatch(t *testing.T) {
	objSize := uint8(5)
	objsPerSlab := uint(30)
	slabs := uint(100)
	Convey("When creating a new slab pool", t, func() {
		sp := NewSlabPool(objSize, objsPerSlab)

		Convey(fmt.Sprintf("And adding %d objects to it", objsPerSlab*slabs), func() {
			for i := uint(0); i < objsPerSlab*slabs; i++ {
				sp.add([]byte(fmt.Sprintf("%05d", i)))
			}

			Convey("Then we should be able to search for the objects in batches", func() {
				searchTerms := []string{"01473", "00831", "00000", "02999", "01234", "02222"}

				batch := make([][]byte, len(searchTerms))
				for i, term := range searchTerms {
					batch[i] = []byte(term)
				}
				searchResult := sp.searchBatched(batch)
				searchResultStrings := make([]string, len(searchResult))
				for i := range searchResult {
					if searchResult[i] == 0 {
						continue
					}
					searchResultStrings[i] = string(objFromObjAddr(searchResult[i], 5))
				}

				for i, result := range searchResultStrings {
					if result != searchTerms[i] {
						fmt.Println(fmt.Sprintf("\nsearches: %+v\nresults: %+v", searchTerms, searchResultStrings))
						t.Fatalf("Results and search terms are expected to match, but they don't")
					}
				}
			})
		})
	})
}

func BenchmarkAddingSearchingObjectInLargePool(b *testing.B) {
	objSize := uint8(20)
	objsPerSlab := uint(100)
	sp := NewSlabPool(objSize, objsPerSlab)
	type valueAndAddr struct {
		value []byte
		addr  ObjAddr
	}
	valueCount := 100000
	testValues := make([]valueAndAddr, valueCount)
	for i := 0; i < valueCount; i++ {
		value := []byte(fmt.Sprintf("%20d", i+valueCount))
		addr, _, err := sp.add([]byte(value))
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
