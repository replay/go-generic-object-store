package gos

// Config provides an ObjectStoreConfig with default settings.
var Config = NewConfig()

// ObjectStoreConfig is used by the object store when creating a new instance.
// Please see the documentation at https://github.com/replay/go-generic-object-store
// for more information
type ObjectStoreConfig struct {
	BaseObjectsPerSlab uint
	GrowthExponent     float64 // for use with math.Pow this is easier
	GrowthFactor       float64 // ^^
}

// NewConfig returns a new object store configuration with
// default settings. Please see the documentation at
// https://github.com/replay/go-generic-object-store for
// more information.
func NewConfig() ObjectStoreConfig {
	return ObjectStoreConfig{
		BaseObjectsPerSlab: 25,
		GrowthExponent:     5,
		GrowthFactor:       1.3,
	}
}
