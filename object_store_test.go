package gos

import (
	"fmt"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

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
				res := os.Get(addr)
				So(string(res), ShouldEqual, obj)
			}

			Convey("there should be 3 different slab pools", func() {
				So(len(os.slabPools), ShouldEqual, 3)
			})
		})
	})
}
