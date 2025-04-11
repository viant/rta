package registry

import "github.com/viant/rta/collector"

// mapperFn defines the signature of mapper functions.
type mapperFn func(acc *collector.Accumulator) interface{}

// MapperFnRegistry maps names to mapper functions.
type MapperFnRegistry map[string]mapperFn

// Default registry instance for mapper functions.
var mapperFnRegistry = MapperFnRegistry{}

// Register adds a mapper function into the registry under the provided name.
func (r MapperFnRegistry) Register(name string, fn mapperFn) {
	r[name] = fn
}

// Lookup retrieves a mapper function by its name.
func (r MapperFnRegistry) Lookup(name string) (mapperFn, bool) {
	fn, ok := r[name]
	return fn, ok
}

// LookupMapperFn is a convenient global helper for retrieving mapper functions from the default registry.
func LookupMapperFn(name string) (mapperFn, bool) {
	return mapperFnRegistry.Lookup(name)
}
