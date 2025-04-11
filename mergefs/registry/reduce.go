package registry

// reduceFn defines the signature of a reduce function.
type reduceFn func(accumulator interface{}, source interface{})

// ReduceFnRegistry maps names to reduce functions.
type ReduceFnRegistry map[string]reduceFn

// Global registry instance for reduce functions.
var reduceFnRegistry = ReduceFnRegistry{}

// Register adds a reduce function into the registry under the provided name.
func (r ReduceFnRegistry) Register(name string, fn reduceFn) {
	r[name] = fn
}

// Lookup retrieves a reduce function by its name.
func (r ReduceFnRegistry) Lookup(name string) (reduceFn, bool) {
	fn, ok := r[name]
	return fn, ok
}

// LookupReduceFn is a global helper function to lookup reduce functions from the default registry.
func LookupReduceFn(name string) (reduceFn, bool) {
	return reduceFnRegistry.Lookup(name)
}
