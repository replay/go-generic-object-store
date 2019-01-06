package gos

import (
	"fmt"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestAddingDeletingSlabs(t *testing.T) {
	objSize := uint8(10)
	objsPerSlab := uint(1)
	sp := NewSlabPool(objSize, objsPerSlab)
	var objAddresses []ObjAddr

	Convey("When adding 3 times as many objects as there are objects per slab", t, func() {
		for i := 0; i < int(objsPerSlab)*3; i++ {
			value := fmt.Sprintf("%010d", i)
			objAddr, _, _ := sp.add([]byte(value))
			objAddresses = append(objAddresses, objAddr)
		}

		So(len(sp.slabs), ShouldEqual, 3)

		/*Convey("then we delete all objects again", func() {
			for _, objAddr := range objAddresses {
				err := sp.delete(objAddr)
				So(err, ShouldBeNil)
			}

			So(len(sp.slabs), ShouldEqual, 0)
		})*/
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

func TestDeletingAddedObjects(t *testing.T) {
	testValue := "abcde"
	objSize := uint8(5)
	objsPerSlab := uint(1)
	sp := NewSlabPool(objSize, objsPerSlab)

	Convey("When adding and object to the pool", t, func() {
		_, _, err := sp.add([]byte(testValue))
		So(err, ShouldBeNil)

		Convey("then we should be able to retreive it by searching for the value and getting the id", func() {
			searchResult, success := sp.search([]byte(testValue))
			So(success, ShouldBeTrue)
			returnedValue := sp.get(searchResult)
			So(returnedValue, ShouldResemble, []byte(testValue))
			returnedValue = sp.get(searchResult)
			So(returnedValue, ShouldResemble, []byte(testValue))

			/*Convey("Then we delete that object by id", func() {
				err = sp.delete(objAddr)
				So(err, ShouldBeNil)

				Convey("now we we should not be able to retrieve it by searching for the value anymore", func() {
					_, success := sp.search([]byte(testValue))
					So(success, ShouldBeFalse)
				})
			})*/
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
