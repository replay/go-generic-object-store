# go-generic-object-store

## Introduction

go-generic-object-store is a small, fast, light-weight, in-memory, off-heap library designed specifically for use with string interning and other similar redundancy reduction concepts.

## Design

### Object Store
At the top level of the library is the Object Store. You can have multiple instances of different `ObjectStore`s. Each `ObjectStore` has a pool of slabs and a lookup table. The following diagram should help illuminate these concepts.

![object store diagram](object_store.png)

#### Slab Pools

`slabPools` is a `map[uint8]slabPool`. The slab pools are indexed by the size (in bytes) of the data stored in the pool. Once a slab in the pool is filled another will be created when necessary. There is no soft limit to the number of slabs that can exist in a single slab pool.

#### Slab Info
`slabInfo` is a struct which has `start` and `size`.
* `start` is a `SlabAddr` which is actually a `uintptr`. It is used to store the memory address  of the first element in a slab's data slice.
* `size` is a `uint8` and stores the size associated with the slab pool to which the slab is associated.

#### Lookup Table
`lookupTable` is a `[]slabInfo`

## Limitations

* 255 maximum bytes per object stored
* 255 maximum number of objects per slab
