package main

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestAddingObjects(t *testing.T) {
	sp := sizedPool{objSize: 5}
	testString := "abcde"
	Convey("When adding a byte slice to the pool", t, func() {
		pooledBytes, _ := sp.add([]byte(testString))
		Convey("the returned bytes should have the same value", func() {
			So(string(pooledBytes), ShouldEqual, testString)
		})
	})
}
