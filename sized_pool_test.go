package main

import (
	"fmt"
	"reflect"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestAddingObject(t *testing.T) {
	sp := sizedPool{objSize: 5}
	testString := "abcde"
	Convey("When adding a byte slice to the pool", t, func() {
		pooledBytes, _ := sp.add([]byte(testString))
		Convey("the returned bytes should have the same value", func() {
			So(pooledBytes, ShouldResemble, []byte(testString))
		})
	})
}

func TestSearchingObject(t *testing.T) {
	sp := sizedPool{objSize: 5}
	testString := "abcde"
	Convey("When adding a byte slice to the pool", t, func() {
		pooledBytes, _ := sp.add([]byte(testString))
		Convey("we should be able to find it with the search method", func() {
			result, success := sp.search([]byte("abcde"))
			So(success, ShouldBeTrue)
			So(result, ShouldResemble, pooledBytes)

			// verify that pooledBytes and result have the same underlying data
			pooledAddr := reflect.ValueOf(pooledBytes).Pointer()
			resultAddr := reflect.ValueOf(result).Pointer()
			So(resultAddr, ShouldEqual, pooledAddr)
		})
	})
}
func TestAddingMoreObjectsThanFitInOneSlab(t *testing.T) {
	sp := sizedPool{objSize: 5}
	Convey(fmt.Sprintf("When adding %d byte slices to the pool with %d objects per slab", objectsPerSlab+1, objectsPerSlab), t, func() {
		for i := 10000; i <= 10000+int(objectsPerSlab); i++ {
			toInsert := []byte(fmt.Sprintf("%d", i))
			sp.add(toInsert)
		}
		Convey("then the number of slabs should be 2", func() {
			So(len(sp.slabs), ShouldEqual, 2)
		})
	})
}
