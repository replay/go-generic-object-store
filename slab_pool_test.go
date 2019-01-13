package gos

import (
	"fmt"
	"math/rand"
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
				err := sp.delete(objslab.obj, objslab.slab)
				So(err, ShouldBeNil)
			}

			So(len(sp.slabs), ShouldEqual, 0)
		})
	})
}
func TestAddingGettingManyObjects(t *testing.T) {
	objSize := uint8(10)
	objsPerSlab := uint(10)
	sp := NewSlabPool(objSize, objsPerSlab)
	objects := make(map[string]ObjAddr)

	Convey("When generating a set of many test objects", t, func() {
		var err error
		// generate twice as many test object as there are objects per slab and add them to slabPool
		for i := 0; i < int(objsPerSlab)*2; i++ {
			value := fmt.Sprintf("%010d", i)
			objects[value], _, err = sp.add([]byte(value))
			So(err, ShouldBeNil)
		}

		Convey("We should be able to retreive each of them and get the correct value back", func() {
			var returned []byte
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
	sp := NewSlabPool(objSize, objsPerSlab)
	testString1 := "abcde"
	testString2 := "aaaaa"
	var objAddr1, objAddr2 ObjAddr
	var success bool
	Convey("When adding a byte slice to the pool", t, func() {
		sp.add([]byte(testString1))

		Convey("we should be able to find it with the search method", func() {
			objAddr1, success = sp.search([]byte(testString1))
			So(success, ShouldBeTrue)
			result1 := sp.get(objAddr1)
			So(string(result1), ShouldEqual, testString1)
		})
	})
	Convey("When adding a second object", t, func() {
		sp.add([]byte(testString2))

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
	sp := NewSlabPool(objSize, objsPerSlab)
	Convey(fmt.Sprintf("When adding %d objects to the pool", objsPerSlab*expectedSlabs), t, func() {
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
