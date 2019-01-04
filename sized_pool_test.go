package main

import (
	"fmt"
	"reflect"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestAddingGettingManyObjects(t *testing.T) {
	sp := sizedPool{objSize: 5}
	objects := make(map[string]objectID)

	Convey("When generating a set of many test objects", t, func() {
		var err error
		// generate twice as many test object as there are objects per slab and add them to sizedPool
		for i := 0; i < int(objectsPerSlab)*2; i++ {
			value := fmt.Sprintf("%05d", i)
			objects[value], err = sp.add([]byte(value))
			So(err, ShouldBeNil)
		}

		Convey("We should be able to retreive each of them and get the correct value back", func() {
			var returned []byte
			var success bool
			for value, objId := range objects {
				returned, success = sp.get(objId)
				So(success, ShouldBeTrue)
				So(string(returned), ShouldEqual, value)
			}
			So(len(sp.slabs), ShouldEqual, 2)
		})
	})
}

func TestAddingSearchingObject(t *testing.T) {
	sp := sizedPool{objSize: 5}
	testString := "abcde"
	testString2 := "aaaaa"
	var result1, result2 objectID
	var success bool
	Convey("When adding a byte slice to the pool", t, func() {
		sp.add([]byte(testString))

		Convey("we should be able to find it with the search method", func() {
			result1, success = sp.search([]byte(testString))
			So(success, ShouldBeTrue)
			So(sp.slabs[0].free.isUsed(result1.objectPos), ShouldBeTrue)
			So(result1.slabAddr, ShouldEqual, reflect.ValueOf(sp.slabs[0].data).Pointer())
		})
	})
	Convey("When adding a second object", t, func() {
		sp.add([]byte(testString2))

		Convey("we should also be able to find it", func() {
			result2, success = sp.search([]byte(testString2))
			So(success, ShouldBeTrue)
			So(sp.slabs[0].free.isUsed(result2.objectPos), ShouldBeTrue)
			So(result2.slabAddr, ShouldEqual, reflect.ValueOf(sp.slabs[0].data).Pointer())
		})
	})
}

func TestAddingMoreObjectsThanFitInOneSlab(t *testing.T) {
	Convey(fmt.Sprintf("When adding %d byte slices to the pool with %d objects per slab", objectsPerSlab+1, objectsPerSlab), t, func() {
		sp := sizedPool{objSize: 5}
		// generating lots of 5-byte strings
		for i := 10000; i <= 10000+int(objectsPerSlab); i++ {
			toInsert := []byte(fmt.Sprintf("%d", i))
			sp.add(toInsert)
		}

		Convey("then the number of slabs should be 2", func() {
			So(len(sp.slabs), ShouldEqual, 2)
		})
	})
}

func TestDeletingAddedObjects(t *testing.T) {
	testValue := "abcde"
	Convey("When adding and object to the pool", t, func() {
		sp := sizedPool{objSize: 5}
		idFromAdd, err := sp.add([]byte(testValue))
		So(err, ShouldBeNil)

		Convey("then we should be able to retreive it by searching for the value and getting the id", func() {
			idFromSearch, success := sp.search([]byte(testValue))
			So(success, ShouldBeTrue)
			returnedValue, success := sp.get(idFromSearch)
			So(success, ShouldBeTrue)
			So(returnedValue, ShouldResemble, []byte(testValue))
			returnedValue, success = sp.get(idFromAdd)
			So(success, ShouldBeTrue)
			So(returnedValue, ShouldResemble, []byte(testValue))

			Convey("Then we delete that object by id", func() {
				err = sp.delete(idFromAdd)
				So(err, ShouldBeNil)

				Convey("now we we should not be able to retrieve it by searching for the value", func() {
					_, success := sp.search([]byte(testValue))
					So(success, ShouldBeFalse)

					Convey("nor by getting the objectID", func() {
						_, success = sp.get(idFromAdd)
						So(success, ShouldBeFalse)
					})
				})
			})
		})
	})
}
